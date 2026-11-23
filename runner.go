package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"
)

type objectStore interface {
	verify(context.Context, string, fingerprint) error
	upload(context.Context, string, fingerprint, io.ReaderAt) error
}

type runner struct {
	root      *os.Root
	store     objectStore
	log       *slog.Logger
	capBytes  int64
	settle    time.Duration
	dryRun    bool
	recent    map[string]confirmation
	protected []fs.FileInfo
}

type confirmation struct {
	info fs.FileInfo
	time time.Time
}

func (r *runner) scanOnce(ctx context.Context) error {
	recoveryErr := r.recoverStaged()
	scan, err := scanDirectory(ctx, r.root, r.protected)
	if err != nil {
		return errors.Join(recoveryErr, fmt.Errorf("scan watched directory: %w", err))
	}
	overCap := r.capBytes > 0 && scan.bytes > r.capBytes
	files := make([]string, 0, len(scan.files))
	for name := range scan.files {
		files = append(files, name)
	}
	slices.SortFunc(files, func(a, b string) int {
		if order := scan.files[a].ModTime().Compare(scan.files[b].ModTime()); order != 0 {
			return order
		}
		return strings.Compare(a, b)
	})

	now := time.Now()
	recent := make(map[string]confirmation, len(r.recent))
	pending, failed := 0, 0
	for _, name := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		info := scan.files[name]
		if now.Sub(info.ModTime()) < r.settle {
			pending++
			continue
		}
		// Over the cap, rehash and verify every file before considering cleanup.
		if previous, ok := r.recent[name]; ok && !overCap && !r.dryRun &&
			now.Sub(previous.time) < 5*time.Minute && sameFile(previous.info, info) {
			recent[name] = previous
			continue
		}
		err := r.syncFile(ctx, name, info)
		switch {
		case err == nil:
			recent[name] = confirmation{info, time.Now()}
		case errors.Is(err, errObjectMissing), errors.Is(err, errFileChanged), errors.Is(err, fs.ErrNotExist):
			pending++
		default:
			failed++
			r.log.Warn("file could not be verified", "file", name, "error", err)
		}
	}
	r.recent = recent
	r.log.Info("scan complete", "files", len(files), "bytes", scan.bytes, "pending", pending, "failed", failed)
	if failed > 0 {
		err = fmt.Errorf("%d file(s) could not be verified", failed)
	}
	if !overCap {
		return errors.Join(recoveryErr, err)
	}
	if pending > 0 || err != nil || recoveryErr != nil {
		r.log.Warn("over storage cap, cleanup deferred", "bytes", scan.bytes, "cap_bytes", r.capBytes)
		return errors.Join(recoveryErr, err)
	}
	current, err := scanDirectory(ctx, r.root, r.protected)
	if err != nil {
		return err
	}
	if !sameSnapshot(scan.files, current.files) {
		r.log.Warn("over storage cap, cleanup deferred after directory changed")
		return nil
	}
	for _, name := range files {
		if current.bytes <= r.capBytes {
			break
		}
		info := current.files[name]
		if info.Size() == 0 {
			continue
		}
		if r.dryRun {
			r.log.Info("would delete local file", "file", name, "bytes", info.Size())
		} else {
			if err := r.removeVerified(ctx, name, current.files); err != nil {
				return fmt.Errorf("cleanup stopped at %q: %w", name, err)
			}
			delete(recent, name)
			r.log.Info("deleted local file", "file", name, "bytes", info.Size())
		}
		delete(current.files, name)
		current.bytes -= info.Size()
	}
	return nil
}

func (r *runner) syncFile(ctx context.Context, name string, info fs.FileInfo) error {
	file, err := openFile(r.root, name, info)
	if err != nil {
		return err
	}
	defer file.Close()
	id, err := identifyFile(ctx, file, info.Size())
	if err != nil {
		return err
	}
	if err := checkOpenFile(r.root, name, file, info); err != nil {
		return err
	}
	err = r.store.verify(ctx, name, id)
	if errors.Is(err, errObjectMissing) {
		if r.dryRun {
			r.log.Info("would upload file", "file", name, "bytes", id.size)
			return err
		}
		if err := r.store.upload(ctx, name, id, file); err != nil {
			return err
		}
		if err := r.store.verify(ctx, name, id); err != nil {
			if errors.Is(err, errObjectMissing) {
				return errors.New("S3 object could not be found after upload")
			}
			return fmt.Errorf("verify after upload: %w", err)
		}
		r.log.Info("uploaded and verified", "file", name, "bytes", id.size)
	} else if err != nil {
		return err
	}
	return checkOpenFile(r.root, name, file, info)
}
