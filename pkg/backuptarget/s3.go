package backuptarget

// s3.go : S3 / S3-compatible backup target implementation. Speaks the
// standard AWS S3 protocol via aws-sdk-go-v2 — works with native AWS S3
// and with the S3-gateway pair the openweft policy ships :
//
//   * CubeFS (the cluster's shared-storage substrate ; presents an S3
//     API on top of its object/cluster file space) — apache-2.0,
//     CNCF graduated.
//   * versitygw (a thin S3 façade over POSIX, CubeFS, ScoutFS, …) —
//     apache-2.0.
//
// Both are fully open source. MinIO is deliberately not supported by
// openweft's policy ; the AGPLv3+commercial dual-license + recent
// upstream changes don't fit the "ship freely, redistribute freely"
// goal.
//
// URL shape : "s3://bucket@region/prefix". Region is part of the
// authority — the SDK uses it to construct the endpoint URL when no
// override is provided.
//
// Credentials follow the SDK's default chain : env vars (AWS_ACCESS_KEY_ID
// + AWS_SECRET_ACCESS_KEY), shared credentials file, IAM role, etc. No
// custom auth knobs here ; the SDK does the right thing.
//
// Endpoint override : $AWS_ENDPOINT_URL_S3 (e.g.
// "https://versitygw.internal:8000" or "https://cubefs-objectnode.internal")
// swings the client at the right gateway without touching the URL.
// Path-style addressing is the default to play nicely with versitygw /
// CubeFS objectnode which often don't have wildcard DNS for virtual-host
// buckets.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// envEndpointURL is the SDK-standard endpoint override env (alongside
// service-specific AWS_ENDPOINT_URL_S3 which the SDK reads natively).
const envEndpointURL = "AWS_ENDPOINT_URL_S3"

// s3Target binds one S3 endpoint at construction. Bucket and region are
// fixed for the lifetime of the Target ; per-call URLs supply the object
// key (the rest of the path).
type s3Target struct {
	bucket   string
	region   string
	basePath string // the "/prefix" part of the URL — joined with per-call keys
}

func newS3Target(u *url.URL) (Target, error) {
	// URL shape : "s3://bucket@region/prefix"
	//   u.User.Username()   → bucket
	//   u.Host              → region
	//   u.Path              → /prefix
	bucket := u.User.Username()
	if bucket == "" {
		// Fallback : "s3://bucket/prefix" (no region in URL) — region
		// then comes from AWS_REGION / AWS_DEFAULT_REGION.
		bucket = u.Host
		return &s3Target{
			bucket:   bucket,
			region:   "",
			basePath: strings.TrimPrefix(u.Path, "/"),
		}, nil
	}
	return &s3Target{
		bucket:   bucket,
		region:   u.Host,
		basePath: strings.TrimPrefix(u.Path, "/"),
	}, nil
}

func (t *s3Target) Scheme() string { return SchemeS3 }

// client constructs an aws-sdk-go-v2 S3 client honouring our endpoint
// override + path-style addressing default. Cheap to call — the SDK's
// internal HTTP transport is reused across instances of the same shape.
func (t *s3Target) client(ctx context.Context) (*s3.Client, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if t.region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(t.region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	s3Opts := func(o *s3.Options) {
		o.UsePathStyle = true
		if endpoint := os.Getenv(envEndpointURL); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	}
	return s3.NewFromConfig(cfg, s3Opts), nil
}

// keyFor extracts the S3 object key from a per-call URL. The URL's path
// segments after the bucket/region authority are the key.
func (t *s3Target) keyFor(fullURL string) (string, error) {
	u, err := url.Parse(fullURL)
	if err != nil {
		return "", fmt.Errorf("parse url %q: %w", fullURL, err)
	}
	if u.Scheme != SchemeS3 {
		return "", fmt.Errorf("s3 target got non-s3 URL %q", fullURL)
	}
	return strings.TrimPrefix(u.Path, "/"), nil
}

func (t *s3Target) Push(ctx context.Context, fullURL string, size int64, body io.Reader) error {
	key, err := t.keyFor(fullURL)
	if err != nil {
		return err
	}
	cli, err := t.client(ctx)
	if err != nil {
		return err
	}
	// Use the manager Uploader for chunked uploads — needed when size
	// exceeds the SDK's PutObject single-shot ceiling (5 GiB) and useful
	// for parallel-part upload even at smaller sizes.
	uploader := manager.NewUploader(cli)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(t.bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	if err != nil {
		return fmt.Errorf("s3 upload s3://%s/%s: %w", t.bucket, key, err)
	}
	_ = size // accepted for interface symmetry ; manager picks part size itself
	return nil
}

func (t *s3Target) Pull(ctx context.Context, fullURL string, dst io.Writer) (int64, error) {
	key, err := t.keyFor(fullURL)
	if err != nil {
		return 0, err
	}
	cli, err := t.client(ctx)
	if err != nil {
		return 0, err
	}
	// io.Writer needs to be io.WriterAt for the downloader's parallel
	// ranged GETs ; wrap in an aws-sdk helper that buffers to memory
	// when the destination isn't WriterAt. For large backups the caller
	// SHOULD pass a real file (which IS WriterAt).
	wa, isWA := dst.(io.WriterAt)
	if !isWA {
		var buf bytes.Buffer
		out, err := cli.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(t.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return 0, fmt.Errorf("s3 get s3://%s/%s: %w", t.bucket, key, err)
		}
		defer out.Body.Close()
		n, err := io.Copy(&buf, out.Body)
		if err != nil {
			return n, err
		}
		m, err := dst.Write(buf.Bytes())
		return int64(m), err
	}
	dl := manager.NewDownloader(cli)
	n, err := dl.Download(ctx, wa, &s3.GetObjectInput{
		Bucket: aws.String(t.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return n, fmt.Errorf("s3 download s3://%s/%s: %w", t.bucket, key, err)
	}
	return n, nil
}

func (t *s3Target) List(ctx context.Context, prefixURL string) ([]Entry, error) {
	prefix, err := t.keyFor(prefixURL)
	if err != nil {
		return nil, err
	}
	cli, err := t.client(ctx)
	if err != nil {
		return nil, err
	}
	var out []Entry
	paginator := s3.NewListObjectsV2Paginator(cli, &s3.ListObjectsV2Input{
		Bucket: aws.String(t.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list s3://%s/%s: %w", t.bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			u := *mustParse(prefixURL)
			u.Path = "/" + aws.ToString(obj.Key)
			e := Entry{
				URL:       u.String(),
				SizeBytes: aws.ToInt64(obj.Size),
			}
			if obj.LastModified != nil {
				e.LastModified = *obj.LastModified
			}
			out = append(out, e)
		}
	}
	return out, nil
}

func (t *s3Target) Delete(ctx context.Context, fullURL string) error {
	key, err := t.keyFor(fullURL)
	if err != nil {
		return err
	}
	cli, err := t.client(ctx)
	if err != nil {
		return err
	}
	_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(t.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// S3 DeleteObject is already idempotent in protocol (200 even
		// on missing) ; treat NoSuchKey defensively for non-conforming
		// gateways.
		var notFound *s3types.NoSuchKey
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("s3 delete s3://%s/%s: %w", t.bucket, key, err)
	}
	return nil
}
