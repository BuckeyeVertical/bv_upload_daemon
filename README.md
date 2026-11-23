# BV Upload Daemon

Upload a local directory to an Amazon S3 general-purpose bucket. The daemon scans for new and changed files, verifies uploaded bytes, and can reclaim local disk space after every file in the directory has a verified backup.
## Run it

1. Download the archive for your operating system and CPU from [GitHub Releases](https://github.com/BuckeyeVertical/bv_upload_daemon/releases/latest) and extract it. The archive includes a standalone executable.
2. Copy `config.example.toml` to `config.toml`. Set `directory`, `bucket`, and `region`. Keep the config and executable outside the directory you want to sync.
3. Make AWS credentials available through an AWS profile, environment variables, or an IAM role. Set `AWS_PROFILE` to choose a named profile.
4. Preview with `./bv-upload-daemon --config config.toml --dry-run`. Start the continuous runner with `./bv-upload-daemon --config config.toml`. On Windows, use `.\bv-upload-daemon.exe` in place of `./bv-upload-daemon`. Use `--once` to run a single scan with uploads and configured cleanup.

Give the runner `s3:ListBucket` on the bucket so it can distinguish a missing object from a permissions error. Give it `s3:GetObject`, `s3:PutObject`, and `s3:AbortMultipartUpload` on objects in the configured prefix. If the bucket uses SSE-KMS, the runner also needs the applicable KMS permissions to upload and retrieve checksums. The runner performs no S3 deletions.

## Upload and cleanup behavior

- The runner scans regular files recursively and skips symbolic links and its own state directory. A relative `directory` is resolved from the config file's directory. Each S3 key is `<prefix>/<SHA-256>/<relative path>`, with the prefix omitted if empty. For example, `jobs/123/frame.jpg` keeps its relative path below the checksum. If that filename is reused with different contents, both uploads remain available.
- `scan_interval` defaults to `15s`. Files with a modification time within `settle_time` (default `30s`) are treated as pending. For a reliable handoff, write and close each file outside the watched directory, then move it into the directory on the same filesystem. Keep published files immutable while the runner is active.
- `soft_cap_gb` uses decimal gigabytes. For example, `30` means 30,000,000,000 bytes. The runner sums file lengths, and zero or an omitted cap disables cleanup. When over the cap, the runner waits until every managed file in a fresh scan is verified in S3. One pending, failed, or changing file defers all cleanup. Once ready, it removes the oldest verified local files by modification time until the total reaches the cap or falls below it.
- The runner stages each deletion under `.bv-upload-daemon/pending`, rechecks the staged file against S3, and checks the directory again before unlinking it. An interrupted cleanup is restored on startup. If the original path has been reused, the staged file stays under `.bv-upload-daemon/pending` and the runner logs its location for manual recovery. Cleanup requires a filesystem that supports same-volume moves and hard links for recovery.
- A dry run checks S3 and reports proposed uploads. If every file is already verified and the cap is exceeded, it reports the files it would remove. Dry runs leave local files and S3 objects alone.
- S3 stores the checksum record. After a restart, the daemon checks the same content-based key and reuses a matching object. Uploads and verification failures retry on subsequent scans. Consider an S3 lifecycle rule that aborts incomplete multipart uploads after a hard stop.

## Build and release

`go test ./...` runs the tests. Build locally with `go build -o bv-upload-daemon .` using Go 1.26.

Each successful push to `main` publishes a commit-tagged [GitHub Release](https://github.com/BuckeyeVertical/bv_upload_daemon/releases) with standalone Linux, macOS (`darwin`), and Windows binaries for x64 (`amd64`) and ARM64. The release includes `SHA256SUMS` for verifying the archive downloads.
