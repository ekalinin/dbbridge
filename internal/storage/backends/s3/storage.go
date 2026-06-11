package s3

import (
	"context"
	"fmt"
	"io"
	"sync"

	"dbbridge/internal/core/domain"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3ResultStore struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

func NewS3ResultStore(ctx context.Context, bucket, region, endpoint, keyID, secret string) (*S3ResultStore, error) {
	var opts []func(*config.LoadOptions) error
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	if keyID != "" && secret != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(keyID, secret, "")))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})

	uploader := manager.NewUploader(s3Client)

	return &S3ResultStore{
		client:   s3Client,
		uploader: uploader,
		bucket:   bucket,
	}, nil
}

type pipeWriter struct {
	pw       *io.PipeWriter
	wg       sync.WaitGroup
	err      error
	uploadID string
}

func (w *pipeWriter) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

func (w *pipeWriter) Close() error {
	err := w.pw.Close()
	w.wg.Wait()
	if w.err != nil {
		return w.err
	}
	return err
}

func (s *S3ResultStore) Writer(ctx context.Context, queryID string, format string) (io.WriteCloser, domain.ResultRef, error) {
	key := fmt.Sprintf("results/%s.%s", queryID, format)

	pr, pw := io.Pipe()
	pwWrapper := &pipeWriter{pw: pw}
	// Async upload from the pipe reader
	pwWrapper.wg.Go(func() {
		_, err := s.uploader.Upload(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
			Body:   pr,
		})
		if err != nil {
			pwWrapper.err = err
			_ = pr.CloseWithError(err)
		} else {
			_ = pr.Close()
		}
	})

	ref := domain.ResultRef{
		Backend: "s3",
		Locator: key,
		Format:  format,
	}

	return pwWrapper, ref, nil
}

func (s *S3ResultStore) Reader(ctx context.Context, ref domain.ResultRef) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(ref.Locator),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 download failed: %w", err)
	}
	return output.Body, nil
}

func (s *S3ResultStore) Delete(ctx context.Context, ref domain.ResultRef) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(ref.Locator),
	})
	if err != nil {
		return fmt.Errorf("s3 delete failed: %w", err)
	}
	return nil
}
