package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var errObjectMissing = errors.New("S3 object is missing")

type s3Store struct {
	client *s3.Client
	bucket string
	prefix string
}

func (store *s3Store) key(name string, id fingerprint) (string, error) {
	key := id.digest + "/" + filepath.ToSlash(name)
	if store.prefix != "" {
		key = store.prefix + "/" + key
	}
	if len(key) > 1024 || !utf8.ValidString(key) {
		return "", fmt.Errorf("S3 key must be valid UTF-8 and at most 1024 bytes")
	}
	return key, nil
}

func (store *s3Store) verify(ctx context.Context, name string, id fingerprint) error {
	key, err := store.key(name, id)
	if err != nil {
		return err
	}
	result, err := store.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       &store.bucket,
		Key:          &key,
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if hasStatus(err, http.StatusNotFound) {
		return errObjectMissing
	}
	if err != nil {
		return fmt.Errorf("verify S3 object %q: %w", key, err)
	}
	if result.ContentLength == nil || *result.ContentLength != id.size ||
		aws.ToString(result.ChecksumSHA256) != id.checksum {
		return fmt.Errorf("S3 object %q has a missing or mismatched size or SHA-256 checksum", key)
	}
	return nil
}

func (store *s3Store) upload(ctx context.Context, name string, id fingerprint, body io.ReaderAt) error {
	key, err := store.key(name, id)
	if err != nil {
		return err
	}
	if len(id.partSums) == 0 {
		_, err = store.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:            &store.bucket,
			Key:               &key,
			Body:              io.NewSectionReader(body, 0, id.size),
			ContentLength:     &id.size,
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
			ChecksumSHA256:    &id.checksum,
			IfNoneMatch:       aws.String("*"),
		})
	} else {
		err = store.uploadMultipart(ctx, key, id, body)
	}
	// Another runner can finish the same object first. The caller still verifies it.
	if err != nil && !hasStatus(err, http.StatusPreconditionFailed) {
		return fmt.Errorf("upload S3 object %q: %w", key, err)
	}
	return nil
}

func (store *s3Store) uploadMultipart(ctx context.Context, key string, id fingerprint, body io.ReaderAt) (err error) {
	started, err := store.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:            &store.bucket,
		Key:               &key,
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumType:      types.ChecksumTypeComposite,
	})
	if err != nil {
		return err
	}
	if aws.ToString(started.UploadId) == "" {
		return errors.New("S3 did not return a multipart upload ID")
	}
	defer func() {
		if err == nil {
			return
		}
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		_, abortErr := store.client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket:   &store.bucket,
			Key:      &key,
			UploadId: started.UploadId,
		})
		if abortErr != nil && !hasStatus(abortErr, http.StatusNotFound) {
			err = errors.Join(err, fmt.Errorf("abort multipart upload: %w", abortErr))
		}
	}()

	parts := make([]types.CompletedPart, 0, len(id.partSums))
	for index, checksum := range id.partSums {
		number := int32(index + 1)
		offset := int64(index) * id.partSize
		length := min(id.partSize, id.size-offset)
		part, err := store.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:            &store.bucket,
			Key:               &key,
			UploadId:          started.UploadId,
			PartNumber:        &number,
			Body:              io.NewSectionReader(body, offset, length),
			ContentLength:     &length,
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
			ChecksumSHA256:    &checksum,
		})
		if err != nil {
			return fmt.Errorf("upload part %d: %w", number, err)
		}
		if aws.ToString(part.ChecksumSHA256) != checksum || aws.ToString(part.ETag) == "" {
			return fmt.Errorf("S3 did not confirm checksum for part %d", number)
		}
		parts = append(parts, types.CompletedPart{PartNumber: &number, ETag: part.ETag, ChecksumSHA256: &checksum})
	}
	_, err = store.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &store.bucket,
		Key:             &key,
		UploadId:        started.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		ChecksumType:    types.ChecksumTypeComposite,
		IfNoneMatch:     aws.String("*"),
	})
	return err
}

func hasStatus(err error, code int) bool {
	var response interface{ HTTPStatusCode() int }
	return errors.As(err, &response) && response.HTTPStatusCode() == code
}
