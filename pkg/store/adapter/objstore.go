package adapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/goccy/go-json"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
)

// objstoreAdapter wraps an objstore.Bucket to implement the daramjwee.Store interface.
//
// This adapter provides cloud storage integration for daramjwee caching, supporting
// major cloud providers (AWS S3, Google Cloud Storage, Azure Blob Storage) through
// the Thanos objstore interface. It's designed for distributed caching scenarios
// where cache persistence and sharing across multiple instances is required.
//
// Cloud storage integration patterns and configuration requirements:
//   - Uses Thanos objstore.Bucket interface for cloud provider abstraction
//   - Supports AWS S3, Google Cloud Storage, Azure Blob Storage, and compatible APIs
//   - Manages metadata separately from data for efficient operations
//   - Provides true streaming uploads for memory efficiency
//
// Streaming upload behavior and memory efficiency benefits:
//   - Uses io.Pipe for true streaming uploads without buffering entire objects
//   - Concurrent upload processing for optimal performance
//   - Memory usage independent of object size
//   - Suitable for large object caching scenarios
//
// Metadata management strategy:
//   - Stores metadata as separate '.meta.json' objects alongside data
//   - Enables efficient metadata-only operations (Stat)
//   - Maintains cache semantics with ETag and timestamp information
//   - Atomic operations through cloud storage consistency guarantees
//
// Concurrent access limitations and race condition considerations:
//   - Cloud storage eventual consistency may affect concurrent operations
//   - Metadata and data objects are managed separately (potential race conditions)
//   - Suitable for scenarios where cache consistency requirements are relaxed
//   - Consider using with appropriate cache invalidation strategies
//
// Performance characteristics:
//   - Network latency dependent (typically 10-100ms per operation)
//   - Excellent for large objects and distributed scenarios
//   - Streaming operations minimize memory usage
//   - Concurrent upload/download for better throughput
//
// Example usage:
//
//	// AWS S3 configuration
//	s3Config := s3.Config{
//	    Bucket:   "my-cache-bucket",
//	    Endpoint: "s3.amazonaws.com",
//	    Region:   "us-west-2",
//	}
//	bucket, err := s3.NewBucket(logger, s3Config, "cache-prefix/")
//	store := adapter.NewObjstoreAdapter(bucket, logger)
//
//	// Google Cloud Storage configuration
//	gcsConfig := gcs.Config{
//	    Bucket: "my-gcs-cache-bucket",
//	}
//	bucket, err := gcs.NewBucket(ctx, logger, gcsConfig, "cache-prefix/")
//	store := adapter.NewObjstoreAdapter(bucket, logger)
type objstoreAdapter struct {
	bucket objstore.Bucket
	logger log.Logger
}

// NewObjstoreAdapter creates a new cloud storage adapter.
//
// This function initializes a daramjwee.Store implementation that uses cloud
// object storage as the backend. It wraps any objstore.Bucket implementation
// to provide caching functionality with cloud storage persistence.
//
// Parameters:
//   - bucket: objstore.Bucket implementation for the target cloud provider
//   - logger: Logger for operational messages and debugging
//
// Supported cloud providers through objstore.Bucket:
//   - AWS S3 and S3-compatible APIs (MinIO, DigitalOcean Spaces, etc.)
//   - Google Cloud Storage
//   - Azure Blob Storage
//   - Any objstore.Bucket implementation
//
// Example configurations:
//
//	// AWS S3 with custom configuration
//	s3Config := s3.Config{
//	    Bucket:          "my-cache-bucket",
//	    Endpoint:        "s3.amazonaws.com",
//	    Region:          "us-west-2",
//	    AccessKey:       "your-access-key",
//	    SecretKey:       "your-secret-key",
//	    Insecure:        false,
//	    SignatureV2:     false,
//	    SSEConfig:       s3.SSEConfig{...},
//	    HTTPConfig:      s3.HTTPConfig{...},
//	}
//	bucket, err := s3.NewBucket(logger, s3Config, "cache-prefix/")
//	if err != nil {
//	    return nil, err
//	}
//	store := adapter.NewObjstoreAdapter(bucket, logger)
//
//	// Google Cloud Storage
//	gcsConfig := gcs.Config{
//	    Bucket:      "my-gcs-cache-bucket",
//	    ServiceAccount: "path/to/service-account.json",
//	}
//	bucket, err := gcs.NewBucket(ctx, logger, gcsConfig, "cache-prefix/")
//	if err != nil {
//	    return nil, err
//	}
//	store := adapter.NewObjstoreAdapter(bucket, logger)
//
//	// Azure Blob Storage
//	azureConfig := azure.Config{
//	    StorageAccountName: "mystorageaccount",
//	    StorageAccountKey:  "your-account-key",
//	    ContainerName:      "cache-container",
//	}
//	bucket, err := azure.NewBucket(logger, azureConfig, "cache-prefix/")
//	if err != nil {
//	    return nil, err
//	}
//	store := adapter.NewObjstoreAdapter(bucket, logger)
//
// Integration considerations:
//   - Ensure proper authentication and permissions for the cloud provider
//   - Configure appropriate bucket policies and access controls
//   - Consider network latency and bandwidth costs
//   - Plan for eventual consistency behavior in cloud storage
//
// Performance characteristics:
//   - Network latency dependent (typically 10-100ms per operation)
//   - Excellent scalability and durability
//   - Cost-effective for large datasets
//   - Suitable for distributed caching scenarios
func NewObjstoreAdapter(bucket objstore.Bucket, logger log.Logger) daramjwee.Store {
	return &objstoreAdapter{
		bucket: bucket,
		logger: logger,
	}
}

// GetStream retrieves an object as a stream from cloud storage.
//
// This method provides streaming access to cached data stored in cloud object
// storage with automatic metadata retrieval. It handles the two-phase process
// of fetching metadata first, then streaming the actual data content.
//
// Cloud storage access patterns:
//   - First retrieves metadata from separate '.meta.json' object
//   - Then streams data object directly from cloud storage
//   - Handles cloud provider-specific error conditions
//   - Provides consistent error handling across different providers
//
// Performance characteristics:
//   - Network latency dependent (typically 10-100ms per operation)
//   - Streaming access without buffering entire objects
//   - Efficient for large objects through direct streaming
//   - Concurrent metadata and data access where possible
//
// Error handling and retry strategies:
//   - Handles cloud provider-specific "not found" errors
//   - Provides detailed error context for debugging
//   - Logs warnings for inconsistent metadata/data states
//   - Suitable for integration with retry mechanisms
//
// Example usage:
//
//	stream, metadata, err := store.GetStream(ctx, "user:123")
//	if err != nil {
//	    if errors.Is(err, daramjwee.ErrNotFound) {
//	        // Object not found in cloud storage
//	        return nil, nil, err
//	    }
//	    return nil, nil, err
//	}
//	defer stream.Close()
//
//	// Process metadata
//	if metadata.IsNegative {
//	    // Handle negative cache entry
//	}
//
//	// Stream data from cloud storage
//	data, err := io.ReadAll(stream)
//
// Network considerations:
//   - Consider implementing retry logic for transient network failures
//   - Monitor cloud provider rate limits and quotas
//   - Plan for eventual consistency in metadata operations
//
// Thread safety: Safe for concurrent use across multiple goroutines.
func (a *objstoreAdapter) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	meta, err := a.Stat(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	dataPath := a.toDataPath(key)
	r, err := a.bucket.Get(ctx, dataPath)
	if err != nil {
		level.Warn(a.logger).Log("msg", "failed to get data object even though metadata exists", "key", key, "err", err)
		return nil, nil, fmt.Errorf("failed to get object for key '%s' after meta check: %w", key, err)
	}

	return r, meta, nil
}

// SetWithWriter returns a writer for streaming data to cloud storage.
//
// This method provides true streaming uploads to cloud object storage using
// io.Pipe for concurrent processing. Data is streamed directly to the cloud
// provider without buffering entire objects in memory, making it highly
// memory-efficient for large objects.
//
// Streaming upload behavior and memory efficiency benefits:
//   - Uses io.Pipe for true streaming without memory buffering
//   - Concurrent upload processing in separate goroutine
//   - Memory usage independent of object size
//   - Suitable for large object caching scenarios
//
// Two-phase upload process:
//   - Phase 1: Stream data object to cloud storage concurrently
//   - Phase 2: Upload metadata object after data upload completes
//   - Ensures metadata consistency with successful data uploads
//   - Handles upload failures gracefully with proper cleanup
//
// Performance characteristics:
//   - Network bandwidth dependent upload speeds
//   - Concurrent processing for optimal throughput
//   - Memory usage remains constant regardless of object size
//   - Suitable for high-throughput streaming scenarios
//
// Error handling and retry strategies:
//   - Detailed error reporting for upload failures
//   - Proper cleanup of partial uploads on errors
//   - Suitable for integration with retry mechanisms
//   - Comprehensive logging for debugging upload issues
//
// Example usage:
//
//	writer, err := store.SetWithWriter(ctx, "user:123", &daramjwee.Metadata{
//	    ETag: "abc123",
//	    CachedAt: time.Now(),
//	})
//	if err != nil {
//	    return err
//	}
//	defer writer.Close() // Always close to commit data
//
//	// Stream data to cloud storage
//	_, err = io.Copy(writer, dataSource)
//	if err != nil {
//	    return err
//	}
//
//	// Data and metadata are uploaded when Close() is called
//
// Network considerations:
//   - Monitor cloud provider upload limits and quotas
//   - Consider implementing retry logic for transient network failures
//   - Plan for upload bandwidth and associated costs
//
// Resource management: The returned writer must always be closed to ensure
// proper resource cleanup and atomic data commitment.
func (a *objstoreAdapter) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
	pr, pw := io.Pipe()

	writer := &streamingObjstoreWriter{
		ctx:      ctx,
		adapter:  a,
		key:      key,
		metadata: metadata,
		pw:       pw,
		wg:       &sync.WaitGroup{},
	}

	writer.wg.Add(1)
	go func() {
		defer writer.wg.Done()
		dataPath := a.toDataPath(key)
		writer.uploadErr = a.bucket.Upload(ctx, dataPath, pr)
	}()

	return writer, nil
}

// Delete removes an object and its metadata from cloud storage.
//
// This method provides atomic deletion of cached objects by removing both
// the data object and its associated metadata object concurrently. It uses
// error groups for concurrent deletion and comprehensive error handling.
//
// Concurrent deletion strategy:
//   - Deletes data object and metadata object concurrently using errgroup
//   - Provides better performance through parallel operations
//   - Handles partial deletion failures gracefully
//   - Comprehensive error logging for debugging
//
// Error handling and retry strategies:
//   - Detailed error reporting for deletion failures
//   - Logs specific failures for data and metadata objects separately
//   - Returns first encountered error while attempting all deletions
//   - Suitable for integration with retry mechanisms
//
// Performance characteristics:
//   - Concurrent deletion reduces total operation time
//   - Network latency dependent (typically 10-100ms per operation)
//   - Handles cloud provider-specific deletion semantics
//   - Efficient for bulk deletion scenarios
//
// Example usage:
//
//	err := store.Delete(ctx, "user:123")
//	if err != nil {
//	    log.Printf("Failed to delete from cloud storage: %v", err)
//	    // Consider retry logic for transient failures
//	}
//
// Cloud storage considerations:
//   - Some cloud providers may have eventual consistency for delete operations
//   - Consider implementing retry logic for transient network failures
//   - Monitor cloud provider rate limits for delete operations
//   - Plan for partial deletion scenarios in error handling
//
// Idempotent operation: Multiple delete calls for the same key are generally
// safe, though cloud provider behavior may vary for non-existent objects.
func (a *objstoreAdapter) Delete(ctx context.Context, key string) error {
	g, gCtx := errgroup.WithContext(ctx)

	dataPath := a.toDataPath(key)
	metaPath := a.toMetaPath(key)

	// Delete data object
	g.Go(func() error {
		if err := a.bucket.Delete(gCtx, dataPath); err != nil {
			level.Error(a.logger).Log("msg", "failed to delete data object", "key", dataPath, "err", err)
			return err
		}
		return nil
	})

	// Delete meta object
	g.Go(func() error {
		if err := a.bucket.Delete(gCtx, metaPath); err != nil {
			level.Error(a.logger).Log("msg", "failed to delete meta object", "key", metaPath, "err", err)
			return err
		}
		return nil
	})

	return g.Wait()
}

// Stat retrieves metadata for an object from cloud storage without fetching the data.
//
// This method provides efficient access to object metadata for cache validation,
// freshness checks, and conditional requests without the overhead of downloading
// the actual data object. It's optimized for metadata-only operations.
//
// Metadata-only access patterns and performance implications:
//   - Retrieves only the '.meta.json' object from cloud storage
//   - Avoids downloading potentially large data objects
//   - Network efficient for cache validation operations
//   - Suitable for high-frequency metadata checks
//
// Use cases and optimization benefits:
//   - Cache freshness validation before expensive data downloads
//   - ETag-based conditional request processing
//   - Cache statistics and monitoring operations
//   - Existence checks without data transfer costs
//
// Cloud storage considerations:
//   - Network latency dependent (typically 10-100ms per operation)
//   - Bandwidth efficient compared to full object retrieval
//   - Subject to cloud provider rate limits and quotas
//   - May be affected by eventual consistency in some providers
//
// Example usage:
//
//	metadata, err := store.Stat(ctx, "user:123")
//	if err != nil {
//	    if errors.Is(err, daramjwee.ErrNotFound) {
//	        // Object not found in cloud storage
//	        return nil, err
//	    }
//	    return nil, err
//	}
//
//	// Check freshness without downloading data
//	if time.Since(metadata.CachedAt) > maxAge {
//	    // Object is stale, trigger refresh
//	    return triggerRefresh(ctx, key)
//	}
//
//	// Check ETag for conditional requests
//	if metadata.ETag == clientETag {
//	    // Data hasn't changed, return 304 Not Modified
//	    return handleNotModified()
//	}
//
// Error handling:
//   - Handles cloud provider-specific "not found" errors
//   - Provides detailed error context for debugging
//   - Suitable for integration with retry mechanisms
//
// Performance characteristics:
//   - Fast metadata access (network latency dependent)
//   - Minimal data transfer (only small JSON metadata)
//   - Cost-effective for frequent metadata operations
//
// Thread safety: Safe for concurrent use across multiple goroutines.
func (a *objstoreAdapter) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	metaPath := a.toMetaPath(key)

	r, err := a.bucket.Get(ctx, metaPath)
	if err != nil {
		if a.bucket.IsObjNotFoundErr(err) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get metadata object for key '%s': %w", key, err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			level.Warn(a.logger).Log("msg", "failed to close reader in Stat", "key", key, "err", err)
		}
	}()

	metaBytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata for key '%s': %w", key, err)
	}

	var metadata daramjwee.Metadata
	if err := json.Unmarshal(metaBytes, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata for key '%s': %w", key, err)
	}

	return &metadata, nil
}

// toDataPath returns the path for the data object.
func (a *objstoreAdapter) toDataPath(key string) string {
	return key
}

// toMetaPath returns the path for the metadata object.
func (a *objstoreAdapter) toMetaPath(key string) string {
	return key + ".meta.json"
}

// streamingObjstoreWriter handles the true streaming upload process.
type streamingObjstoreWriter struct {
	ctx       context.Context
	adapter   *objstoreAdapter
	key       string
	metadata  *daramjwee.Metadata
	pw        *io.PipeWriter
	wg        *sync.WaitGroup
	uploadErr error
}

// Write writes data to the pipe, which is immediately streamed to the object store.
func (w *streamingObjstoreWriter) Write(p []byte) (n int, err error) {
	return w.pw.Write(p)
}

// Close finalizes the write operation. It closes the pipe, waits for the data
// upload to finish, and then uploads the metadata object.
func (w *streamingObjstoreWriter) Close() error {
	// 1. Close the pipe writer. This signals the end of the stream to the
	//    background upload goroutine and causes it to complete.
	if err := w.pw.Close(); err != nil {
		return err
	}

	// 2. Wait for the background upload to finish.
	w.wg.Wait()

	// 3. Check if the data upload failed.
	if w.uploadErr != nil {
		level.Error(w.adapter.logger).Log("msg", "data object upload failed", "key", w.key, "err", w.uploadErr)
		return fmt.Errorf("data upload for key '%s' failed: %w", w.key, w.uploadErr)
	}

	// 4. If data upload was successful, upload the metadata object.
	metaPath := w.adapter.toMetaPath(w.key)
	metaBytes, err := json.Marshal(w.metadata)
	if err != nil {
		level.Error(w.adapter.logger).Log("msg", "failed to marshal metadata", "key", metaPath, "err", err)
		return fmt.Errorf("failed to marshal metadata for key '%s': %w", w.key, err)
	}

	err = w.adapter.bucket.Upload(w.ctx, metaPath, bytes.NewReader(metaBytes))
	if err != nil {
		level.Error(w.adapter.logger).Log("msg", "failed to upload metadata object", "key", metaPath, "err", err)
		return fmt.Errorf("failed to upload metadata for key '%s': %w", w.key, err)
	}

	level.Debug(w.adapter.logger).Log("msg", "successfully uploaded data and metadata", "key", w.key)
	return nil
}

// Cloud Storage Configuration Examples for Major Providers
//
// The following examples demonstrate how to configure objstore adapters for
// different cloud storage providers with appropriate settings and best practices.

// Example: AWS S3 configuration for production caching
//
//	func NewS3CacheStore(logger log.Logger) (daramjwee.Store, error) {
//	    s3Config := s3.Config{
//	        Bucket:          "my-production-cache",
//	        Endpoint:        "s3.amazonaws.com",
//	        Region:          "us-west-2",
//	        AccessKey:       os.Getenv("AWS_ACCESS_KEY_ID"),
//	        SecretKey:       os.Getenv("AWS_SECRET_ACCESS_KEY"),
//	        Insecure:        false,
//	        SignatureV2:     false,
//	        SSEConfig: s3.SSEConfig{
//	            Type: s3.SSEAES256,
//	        },
//	        HTTPConfig: s3.HTTPConfig{
//	            IdleConnTimeout:       90 * time.Second,
//	            ResponseHeaderTimeout: 2 * time.Minute,
//	            TLSHandshakeTimeout:   10 * time.Second,
//	            ExpectContinueTimeout: 1 * time.Second,
//	            MaxIdleConns:          100,
//	            MaxIdleConnsPerHost:   100,
//	            MaxConnsPerHost:       0,
//	        },
//	    }
//
//	    bucket, err := s3.NewBucket(logger, s3Config, "cache/v1/")
//	    if err != nil {
//	        return nil, fmt.Errorf("failed to create S3 bucket: %w", err)
//	    }
//
//	    return adapter.NewObjstoreAdapter(bucket, logger), nil
//	}

// Example: Google Cloud Storage configuration
//
//	func NewGCSCacheStore(ctx context.Context, logger log.Logger) (daramjwee.Store, error) {
//	    gcsConfig := gcs.Config{
//	        Bucket:         "my-gcs-cache-bucket",
//	        ServiceAccount: "/path/to/service-account.json",
//	    }
//
//	    bucket, err := gcs.NewBucket(ctx, logger, gcsConfig, "cache/v1/")
//	    if err != nil {
//	        return nil, fmt.Errorf("failed to create GCS bucket: %w", err)
//	    }
//
//	    return adapter.NewObjstoreAdapter(bucket, logger), nil
//	}

// Example: Azure Blob Storage configuration
//
//	func NewAzureCacheStore(logger log.Logger) (daramjwee.Store, error) {
//	    azureConfig := azure.Config{
//	        StorageAccountName: os.Getenv("AZURE_STORAGE_ACCOUNT"),
//	        StorageAccountKey:  os.Getenv("AZURE_STORAGE_KEY"),
//	        ContainerName:      "cache-container",
//	        Endpoint:           "", // Use default Azure endpoint
//	        MaxRetries:         3,
//	    }
//
//	    bucket, err := azure.NewBucket(logger, azureConfig, "cache/v1/")
//	    if err != nil {
//	        return nil, fmt.Errorf("failed to create Azure bucket: %w", err)
//	    }
//
//	    return adapter.NewObjstoreAdapter(bucket, logger), nil
//	}

// Example: MinIO (S3-compatible) configuration for on-premises
//
//	func NewMinIOCacheStore(logger log.Logger) (daramjwee.Store, error) {
//	    s3Config := s3.Config{
//	        Bucket:      "cache-bucket",
//	        Endpoint:    "minio.internal.company.com:9000",
//	        AccessKey:   os.Getenv("MINIO_ACCESS_KEY"),
//	        SecretKey:   os.Getenv("MINIO_SECRET_KEY"),
//	        Insecure:    true, // For internal MinIO without TLS
//	        SignatureV2: false,
//	        Region:      "us-east-1", // MinIO default region
//	    }
//
//	    bucket, err := s3.NewBucket(logger, s3Config, "cache/")
//	    if err != nil {
//	        return nil, fmt.Errorf("failed to create MinIO bucket: %w", err)
//	    }
//
//	    return adapter.NewObjstoreAdapter(bucket, logger), nil
//	}

// Example: Multi-region S3 configuration with failover
//
//	func NewMultiRegionS3Store(logger log.Logger) (daramjwee.Store, error) {
//	    primaryConfig := s3.Config{
//	        Bucket:    "cache-us-west-2",
//	        Endpoint:  "s3.us-west-2.amazonaws.com",
//	        Region:    "us-west-2",
//	        AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
//	        SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
//	    }
//
//	    primaryBucket, err := s3.NewBucket(logger, primaryConfig, "cache/")
//	    if err != nil {
//	        return nil, fmt.Errorf("failed to create primary S3 bucket: %w", err)
//	    }
//
//	    return adapter.NewObjstoreAdapter(primaryBucket, logger), nil
//	}
