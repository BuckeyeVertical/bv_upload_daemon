package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

var stagingDirectory = filepath.Join(stateDirectory, "pending")

func (r *runner) removeVerified(ctx context.Context, name string, expected map[string]fs.FileInfo) (err error) {
	info := expected[name]
	staged := filepath.Join(stagingDirectory, name)
	if err := r.root.MkdirAll(filepath.Dir(staged), 0o700); err != nil {
		return err
	}
	// Move first so a file arriving at the original path is never unlinked.
	if err := r.root.Rename(name, staged); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, r.restoreFile(name, staged))
		}
	}()

	file, err := openFile(r.root, staged, info)
	if err != nil {
		return err
	}
	id, err := identifyFile(ctx, file, info.Size())
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	if err := r.store.verify(ctx, name, id); err != nil {
		return err
	}

	// A newly arrived or changed file defers cleanup for the whole directory.
	current, err := scanDirectory(ctx, r.root, r.protected)
	if err != nil {
		return err
	}
	delete(expected, name)
	unchanged := sameSnapshot(expected, current.files)
	expected[name] = info
	if !unchanged {
		return errors.New("directory changed during cleanup")
	}
	stagedInfo, err := r.root.Lstat(staged)
	if err != nil {
		return err
	}
	if !sameFile(info, stagedInfo) {
		return errFileChanged
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.root.Remove(staged)
}

func (r *runner) recoverStaged() error {
	info, err := r.root.Lstat(stagingDirectory)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s must be a directory", stagingDirectory)
	}
	staging, err := r.root.OpenRoot(stagingDirectory)
	if err != nil {
		return err
	}
	defer staging.Close()
	return fs.WalkDir(staging.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		name, err := filepath.Localize(path)
		if err != nil {
			return err
		}
		staged := filepath.Join(stagingDirectory, name)
		if r.dryRun {
			return fmt.Errorf("local cleanup awaits recovery of %q", staged)
		}
		if err := r.restoreFile(name, staged); err != nil {
			return err
		}
		r.log.Info("restored file from interrupted cleanup", "file", name)
		return nil
	})
}

func (r *runner) restoreFile(name, staged string) error {
	// A hard link restores without replacing anything written to the original path.
	info, err := r.root.Lstat(staged)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("manual recovery needed for %q, which is no longer a regular file", staged)
	}
	if err := r.root.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return err
	}
	if err := r.root.Link(staged, name); err != nil {
		original, statErr := r.root.Lstat(name)
		if statErr != nil || !os.SameFile(info, original) {
			return fmt.Errorf("restore %q from %q: %w", name, staged, err)
		}
	}
	return r.root.Remove(staged)
}
