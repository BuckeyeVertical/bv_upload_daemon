package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gofrs/flock"
)

const stateDirectory = ".bv-upload-daemon"

func lockDirectory(root *os.Root) (*flock.Flock, error) {
	if err := root.Mkdir(stateDirectory, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("create daemon state directory: %w", err)
	}
	info, err := root.Lstat(stateDirectory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s must be a directory", stateDirectory)
	}
	lock := flock.New(filepath.Join(root.Name(), stateDirectory, "runner.lock"))
	locked, err := lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("lock watched directory: %w", err)
	}
	if !locked {
		return nil, fmt.Errorf("another runner is watching %s", root.Name())
	}
	return lock, nil
}

type snapshot struct {
	files map[string]fs.FileInfo
	bytes int64
}

func scanDirectory(ctx context.Context, root *os.Root, protected []fs.FileInfo) (snapshot, error) {
	result := snapshot{files: make(map[string]fs.FileInfo)}
	err := fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if path == stateDirectory && entry.IsDir() {
			return fs.SkipDir
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		name, err := filepath.Localize(path)
		if err != nil {
			return err
		}
		info, err := root.Lstat(name)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("file changed during scan: %s", name)
		}
		for _, keep := range protected {
			if os.SameFile(keep, info) {
				return nil
			}
		}
		if info.Size() > math.MaxInt64-result.bytes {
			return fmt.Errorf("directory size exceeds supported range")
		}
		result.files[name] = info
		result.bytes += info.Size()
		return nil
	})
	return result, err
}

func sameFile(a, b fs.FileInfo) bool {
	return b.Mode().IsRegular() && os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

func sameSnapshot(expected, found map[string]fs.FileInfo) bool {
	if len(expected) != len(found) {
		return false
	}
	for name, info := range expected {
		if current, ok := found[name]; !ok || !sameFile(info, current) {
			return false
		}
	}
	return true
}

const (
	defaultPartSize = 32 << 20
	maxPartSize     = 5 << 30
	maxParts        = 10_000
)

type fingerprint struct {
	digest   string
	checksum string
	partSize int64
	partSums []string
	size     int64
}

var errFileChanged = errors.New("file changed during processing")

func openFile(root *os.Root, name string, expected fs.FileInfo) (*os.File, error) {
	current, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !sameFile(expected, current) {
		return nil, errFileChanged
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	if err := checkOpenFile(root, name, file, expected); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

func checkOpenFile(root *os.Root, name string, file *os.File, expected fs.FileInfo) error {
	opened, err := file.Stat()
	if err != nil {
		return err
	}
	current, err := root.Lstat(name)
	if err != nil {
		return err
	}
	if !sameFile(expected, opened) || !sameFile(expected, current) {
		return errFileChanged
	}
	return nil
}

func identifyFile(ctx context.Context, file io.Reader, size int64) (fingerprint, error) {
	id := fingerprint{size: size}
	if size > maxPartSize*maxParts {
		return id, fmt.Errorf("file exceeds S3's multipart upload size limit")
	}
	full := sha256.New()
	reader := contextReader{ctx, file}
	if size <= defaultPartSize {
		if _, err := io.CopyN(full, reader, size); err != nil {
			return id, err
		}
		id.checksum = base64.StdEncoding.EncodeToString(full.Sum(nil))
	} else {
		id.partSize = max(defaultPartSize, (size+maxParts-1)/maxParts)
		part := sha256.New()
		composite := sha256.New()
		for remaining := size; remaining > 0; {
			length := min(id.partSize, remaining)
			part.Reset()
			if _, err := io.CopyN(io.MultiWriter(full, part), reader, length); err != nil {
				return id, err
			}
			sum := part.Sum(nil)
			id.partSums = append(id.partSums, base64.StdEncoding.EncodeToString(sum))
			composite.Write(sum)
			remaining -= length
		}
		id.checksum = base64.StdEncoding.EncodeToString(composite.Sum(nil)) + "-" + strconv.Itoa(len(id.partSums))
	}
	id.digest = hex.EncodeToString(full.Sum(nil))
	return id, nil
}

type contextReader struct {
	ctx context.Context
	io.Reader
}

func (r contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(buffer)
}
