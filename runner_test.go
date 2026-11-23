package main

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type memoryStore struct {
	objects      map[string]bool
	refuseUpload string
	onVerify     func(string)
}

func (store *memoryStore) verify(_ context.Context, name string, id fingerprint) error {
	if store.onVerify != nil {
		store.onVerify(name)
	}
	if !store.objects[name+"/"+id.digest] {
		return errObjectMissing
	}
	return nil
}

func (store *memoryStore) upload(_ context.Context, name string, id fingerprint, file io.ReaderAt) error {
	if name == store.refuseUpload {
		return errors.New("upload failed")
	}
	content, err := identifyFile(context.Background(), io.NewSectionReader(file, 0, id.size), id.size)
	if err != nil {
		return err
	}
	if content.checksum != id.checksum {
		return errors.New("wrong content")
	}
	store.objects[name+"/"+id.digest] = true
	return nil
}

func testRunner(t *testing.T) (*runner, *memoryStore) {
	t.Helper()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	store := &memoryStore{objects: make(map[string]bool)}
	return &runner{root: root, store: store, log: slog.New(slog.NewTextHandler(io.Discard, nil))}, store
}

func writeTestFile(t *testing.T, root *os.Root, name, contents string, age time.Duration) {
	t.Helper()
	if err := root.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := root.WriteFile(name, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().Add(-age)
	if err := root.Chtimes(name, stamp, stamp); err != nil {
		t.Fatal(err)
	}
}

func expectFile(t *testing.T, root *os.Root, name, contents string) {
	t.Helper()
	actual, err := root.ReadFile(name)
	if err != nil || string(actual) != contents {
		t.Fatalf("read %q = %q, %v, want %q", name, actual, err, contents)
	}
}

func expectMissing(t *testing.T, root *os.Root, name string) {
	t.Helper()
	if _, err := root.Lstat(name); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat %q = %v, want absent file", name, err)
	}
}

func TestCleanupWaitsForEveryUpload(t *testing.T) {
	r, store := testRunner(t)
	r.capBytes = 8
	r.settle = time.Minute
	ctx := context.Background()
	old := filepath.Join("job", "oldest.jpg")
	writeTestFile(t, r.root, old, "old1", 3*time.Hour)
	writeTestFile(t, r.root, "middle.jpg", "old2", 2*time.Hour)
	writeTestFile(t, r.root, "new.jpg", "new!", 0)

	if err := r.scanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	expectFile(t, r.root, old, "old1")
	expectFile(t, r.root, "middle.jpg", "old2")
	expectFile(t, r.root, "new.jpg", "new!")
	if len(store.objects) != 2 {
		t.Fatalf("uploaded %d files while newest was settling, want 2", len(store.objects))
	}

	writeTestFile(t, r.root, "new.jpg", "new!", time.Hour)
	store.refuseUpload = "new.jpg"
	if err := r.scanOnce(ctx); err == nil {
		t.Fatal("expected upload error")
	}
	expectFile(t, r.root, old, "old1")
	expectFile(t, r.root, "middle.jpg", "old2")
	expectFile(t, r.root, "new.jpg", "new!")

	store.refuseUpload = ""
	if err := r.scanOnce(ctx); err != nil {
		t.Fatal(err)
	}
	expectMissing(t, r.root, old)
	expectFile(t, r.root, "middle.jpg", "old2")
	expectFile(t, r.root, "new.jpg", "new!")
	if len(store.objects) != 3 {
		t.Fatalf("found %d stored files after cleanup, want 3", len(store.objects))
	}
}

func TestCleanupRestoresFileIfAnotherArrives(t *testing.T) {
	r, store := testRunner(t)
	r.capBytes = 4
	writeTestFile(t, r.root, "first", "1234", time.Hour)
	writeTestFile(t, r.root, "second", "5678", 0)
	store.onVerify = func(name string) {
		if _, err := r.root.Lstat(filepath.Join(stagingDirectory, name)); err == nil {
			writeTestFile(t, r.root, "incoming", "new", 0)
		}
	}
	if err := r.scanOnce(context.Background()); err == nil {
		t.Fatal("expected cleanup to stop")
	}
	expectFile(t, r.root, "first", "1234")
	expectFile(t, r.root, "second", "5678")
	expectFile(t, r.root, "incoming", "new")
	expectMissing(t, r.root, filepath.Join(stagingDirectory, "first"))
}

func TestRecoverInterruptedCleanup(t *testing.T) {
	r, _ := testRunner(t)
	staged := filepath.Join(stagingDirectory, "job", "photo")
	writeTestFile(t, r.root, staged, "unlinked later", time.Hour)
	writeTestFile(t, r.root, filepath.Join("job", "photo"), "new arrival", 0)
	if err := r.recoverStaged(); err == nil {
		t.Fatal("expected recovery conflict")
	}
	expectFile(t, r.root, filepath.Join("job", "photo"), "new arrival")
	expectFile(t, r.root, staged, "unlinked later")
	if err := r.root.Rename(filepath.Join("job", "photo"), "other"); err != nil {
		t.Fatal(err)
	}
	if err := r.recoverStaged(); err != nil {
		t.Fatal(err)
	}
	expectFile(t, r.root, filepath.Join("job", "photo"), "unlinked later")
	expectMissing(t, r.root, staged)
}

func TestDryRunAndRestart(t *testing.T) {
	r, store := testRunner(t)
	r.capBytes = 3
	r.dryRun = true
	writeTestFile(t, r.root, "photo", "original", time.Hour)
	if err := r.scanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.objects) != 0 {
		t.Fatal("dry run uploaded a file")
	}
	expectMissing(t, r.root, stateDirectory)
	r.dryRun = false
	r.capBytes = 0
	if err := r.scanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	r.recent = nil
	store.refuseUpload = "photo"
	if err := r.scanOnce(context.Background()); err != nil {
		t.Fatalf("restart tried to upload an already verified file: %v", err)
	}
	store.refuseUpload = ""
	writeTestFile(t, r.root, "photo", "replaced", time.Hour)
	if err := r.scanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.objects) != 2 {
		t.Fatal("a reused path did not preserve both uploaded copies")
	}
	r.dryRun = true
	r.capBytes = 3
	if err := r.scanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	expectFile(t, r.root, "photo", "replaced")
	expectMissing(t, r.root, stateDirectory)
}

func TestReadExampleConfig(t *testing.T) {
	contents, err := os.ReadFile("config.example.toml")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "photos"), 0o700); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(dir, "config.toml")
	contents = []byte(strings.Replace(string(contents), "/path/to/photos", "photos", 1))
	if err := os.WriteFile(filename, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	settings, err := readConfig(filename)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(settings.Directory) != "photos" || settings.capBytes != 30_000_000_000 {
		t.Fatalf("unexpected config: %+v", settings)
	}
}
