package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3ChecksumVerification(t *testing.T) {
	for _, size := range []int{0, 42, defaultPartSize + 7} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			ctx := context.Background()
			data := bytes.NewReader(make([]byte, size))
			id, err := identifyFile(ctx, data, int64(size))
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var recordedChecksum string
			var recordedSize int64
			var parts [][]byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if req.URL.Path != "/test-bucket/prefix/"+id.digest+"/image.jpg" {
					t.Errorf("unexpected S3 path %q", req.URL.Path)
				}
				query := req.URL.Query()
				switch {
				case req.Method == http.MethodHead:
					if req.Header.Get("X-Amz-Checksum-Mode") != "ENABLED" {
						t.Error("HEAD request did not enable checksums")
					}
					if recordedChecksum == "" {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					w.Header().Set("X-Amz-Checksum-Sha256", recordedChecksum)
					w.Header().Set("Content-Length", strconv.FormatInt(recordedSize, 10))
				case req.Method == http.MethodPost && query.Has("uploads"):
					if req.Header.Get("X-Amz-Checksum-Algorithm") != "SHA256" ||
						req.Header.Get("X-Amz-Checksum-Type") != "COMPOSITE" {
						t.Error("multipart upload did not select composite SHA-256")
					}
					fmt.Fprint(w, "<InitiateMultipartUploadResult><UploadId>test-id</UploadId></InitiateMultipartUploadResult>")
				case req.Method == http.MethodPut:
					sum := sha256.New()
					count, err := io.Copy(sum, req.Body)
					if err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					checksum := base64.StdEncoding.EncodeToString(sum.Sum(nil))
					if req.Header.Get("X-Amz-Checksum-Sha256") != checksum {
						t.Error("checksum sent with PUT did not match its body")
					}
					if query.Has("uploadId") {
						parts = append(parts, sum.Sum(nil))
						recordedSize += count
						w.Header().Set("X-Amz-Checksum-Sha256", checksum)
						w.Header().Set("ETag", `"part-etag"`)
					} else {
						if req.Header.Get("If-None-Match") != "*" {
							t.Error("single upload needs conditional creation")
						}
						recordedChecksum = checksum
						recordedSize = count
					}
				case req.Method == http.MethodPost && query.Has("uploadId"):
					if req.Header.Get("If-None-Match") != "*" {
						t.Error("multipart completion needs conditional creation")
					}
					var body struct {
						Parts []struct {
							Number   int    `xml:"PartNumber"`
							Checksum string `xml:"ChecksumSHA256"`
						} `xml:"Part"`
					}
					if err := xml.NewDecoder(req.Body).Decode(&body); err != nil || len(body.Parts) != len(parts) {
						t.Errorf("invalid complete multipart upload: %+v, %v", body, err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					composite := sha256.New()
					for i, part := range parts {
						if body.Parts[i].Number != i+1 || body.Parts[i].Checksum != base64.StdEncoding.EncodeToString(part) {
							t.Error("multipart completion included an invalid part")
						}
						composite.Write(part)
					}
					recordedChecksum = base64.StdEncoding.EncodeToString(composite.Sum(nil)) + "-" + strconv.Itoa(len(parts))
					fmt.Fprint(w, "<CompleteMultipartUploadResult/>")
				default:
					t.Errorf("unexpected S3 request: %s %s", req.Method, req.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			client := s3.New(s3.Options{
				BaseEndpoint: aws.String(server.URL),
				Region:       "us-east-2",
				Credentials:  aws.AnonymousCredentials{},
				UsePathStyle: true,
				Retryer:      aws.NopRetryer{},
			})
			store := &s3Store{client, "test-bucket", "prefix"}
			if err := store.verify(ctx, "image.jpg", id); !errors.Is(err, errObjectMissing) {
				t.Fatalf("missing object verification returned %v", err)
			}
			if err := store.upload(ctx, "image.jpg", id, data); err != nil {
				t.Fatal(err)
			}
			if err := store.verify(ctx, "image.jpg", id); err != nil {
				t.Fatal(err)
			}
			id.checksum = "wrong"
			if err := store.verify(ctx, "image.jpg", id); err == nil {
				t.Fatal("verified an object with a mismatched checksum")
			}
		})
	}
}
