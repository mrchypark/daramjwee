# Custom Compressor Implementation Guidelines

## Overview

This document provides comprehensive guidelines for implementing custom compression algorithms for the daramjwee caching library. A Compressor provides data compression and decompression functionality to reduce storage space and network bandwidth usage while maintaining data integrity and performance.

## Compressor Interface Contract

The Compressor interface defines four core methods that must be implemented:

```go
type Compressor interface {
    Compress(dst io.Writer, src io.Reader) (int64, error)
    Decompress(dst io.Writer, src io.Reader) (int64, error)
    Algorithm() string
    Level() int
}
```

### Method Implementation Requirements

#### Compress Method

**Purpose**: Compress data from source reader and write to destination writer.

**Implementation Requirements**:
- Read data from `src` and write compressed data to `dst`
- Return the number of bytes written to the destination
- Handle streaming data efficiently without loading everything into memory
- Return `daramjwee.ErrCompressionFailed` for compression failures
- Ensure proper resource cleanup on errors

**Example Implementation Pattern**:
```go
func (c *CustomCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    // Create compression writer with appropriate settings
    writer, err := c.createCompressionWriter(dst)
    if err != nil {
        return 0, fmt.Errorf("%w: failed to create compression writer: %v", 
            daramjwee.ErrCompressionFailed, err)
    }
    defer writer.Close()
    
    // Stream data through compressor
    written, err := io.Copy(writer, src)
    if err != nil {
        return written, fmt.Errorf("%w: failed to compress data: %v", 
            daramjwee.ErrCompressionFailed, err)
    }
    
    // Finalize compression
    if err := writer.Close(); err != nil {
        return written, fmt.Errorf("%w: failed to finalize compression: %v", 
            daramjwee.ErrCompressionFailed, err)
    }
    
    return written, nil
}
```

#### Decompress Method

**Purpose**: Decompress data from source reader and write to destination writer.

**Implementation Requirements**:
- Read compressed data from `src` and write decompressed data to `dst`
- Return the number of bytes written to the destination
- Handle streaming decompression efficiently
- Return `daramjwee.ErrDecompressionFailed` for decompression failures
- Validate data integrity during decompression

**Example Implementation Pattern**:
```go
func (c *CustomCompressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
    // Create decompression reader
    reader, err := c.createDecompressionReader(src)
    if err != nil {
        return 0, fmt.Errorf("%w: failed to create decompression reader: %v", 
            daramjwee.ErrDecompressionFailed, err)
    }
    defer reader.Close()
    
    // Stream decompressed data
    written, err := io.Copy(dst, reader)
    if err != nil {
        return written, fmt.Errorf("%w: failed to decompress data: %v", 
            daramjwee.ErrDecompressionFailed, err)
    }
    
    return written, nil
}
```

#### Algorithm Method

**Purpose**: Return the compression algorithm name for identification.

**Implementation Requirements**:
- Return a consistent, unique string identifier
- Use lowercase names for consistency
- Follow standard algorithm naming conventions

**Example Implementation**:
```go
func (c *CustomCompressor) Algorithm() string {
    return "custom-lz77"  // Or whatever your algorithm is called
}
```

#### Level Method

**Purpose**: Return the current compression level setting.

**Implementation Requirements**:
- Return the configured compression level
- Use consistent level ranges across implementations
- Document level meanings and trade-offs

**Example Implementation**:
```go
func (c *CustomCompressor) Level() int {
    return c.compressionLevel
}
```

## Error Handling Patterns

### Standard Error Conditions

The daramjwee library defines several standard compression-related errors:

1. **ErrCompressionFailed**: Compression operation failed
2. **ErrDecompressionFailed**: Decompression operation failed
3. **ErrInvalidCompressionLevel**: Invalid compression level specified
4. **ErrUnsupportedAlgorithm**: Unsupported compression algorithm
5. **ErrCorruptedData**: Corrupted compressed data detected

### Error Handling Examples

```go
// Proper error wrapping for compression failures
func (c *CustomCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    writer, err := c.backend.NewWriter(dst)
    if err != nil {
        return 0, fmt.Errorf("%w: failed to initialize compressor: %v", 
            daramjwee.ErrCompressionFailed, err)
    }
    
    written, err := io.Copy(writer, src)
    if err != nil {
        writer.Close()  // Cleanup on error
        return written, fmt.Errorf("%w: compression failed during data copy: %v", 
            daramjwee.ErrCompressionFailed, err)
    }
    
    if err := writer.Close(); err != nil {
        return written, fmt.Errorf("%w: failed to finalize compression: %v", 
            daramjwee.ErrCompressionFailed, err)
    }
    
    return written, nil
}

// Validation and configuration errors
func NewCustomCompressor(level int) (*CustomCompressor, error) {
    if level < MinCompressionLevel || level > MaxCompressionLevel {
        return nil, fmt.Errorf("%w: level %d not in range [%d, %d]", 
            daramjwee.ErrInvalidCompressionLevel, level, MinCompressionLevel, MaxCompressionLevel)
    }
    
    return &CustomCompressor{level: level}, nil
}

// Data corruption detection
func (c *CustomCompressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
    reader, err := c.backend.NewReader(src)
    if err != nil {
        if c.isCorruptionError(err) {
            return 0, fmt.Errorf("%w: compressed data appears corrupted: %v", 
                daramjwee.ErrCorruptedData, err)
        }
        return 0, fmt.Errorf("%w: failed to initialize decompressor: %v", 
            daramjwee.ErrDecompressionFailed, err)
    }
    
    written, err := io.Copy(dst, reader)
    if err != nil {
        if c.isCorruptionError(err) {
            return written, fmt.Errorf("%w: data corruption detected during decompression: %v", 
                daramjwee.ErrCorruptedData, err)
        }
        return written, fmt.Errorf("%w: decompression failed: %v", 
            daramjwee.ErrDecompressionFailed, err)
    }
    
    return written, nil
}
```

## Performance Optimization Strategies

### Memory Management

```go
// Buffer pooling for compression operations
var compressionBufferPool = sync.Pool{
    New: func() interface{} {
        return make([]byte, 64*1024)  // 64KB buffers
    },
}

func (c *CustomCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    buf := compressionBufferPool.Get().([]byte)
    defer compressionBufferPool.Put(buf)
    
    // Use pooled buffer for compression operations
    return c.compressWithBuffer(dst, src, buf)
}

// Pre-allocated compression contexts
type CustomCompressor struct {
    level   int
    encoder *compressionEncoder  // Reusable encoder
    decoder *compressionDecoder  // Reusable decoder
    mu      sync.Mutex          // Protect reusable components
}

func (c *CustomCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    // Reset and reuse encoder
    c.encoder.Reset(dst)
    defer c.encoder.Reset(nil)  // Clear references
    
    return io.Copy(c.encoder, src)
}
```

### Streaming Optimization

```go
// Chunked processing for large data
func (c *CustomCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    const chunkSize = 1024 * 1024  // 1MB chunks
    
    writer := c.createWriter(dst)
    defer writer.Close()
    
    buf := make([]byte, chunkSize)
    var totalWritten int64
    
    for {
        n, err := src.Read(buf)
        if n > 0 {
            written, writeErr := writer.Write(buf[:n])
            totalWritten += int64(written)
            if writeErr != nil {
                return totalWritten, fmt.Errorf("%w: chunk compression failed: %v", 
                    daramjwee.ErrCompressionFailed, writeErr)
            }
        }
        
        if err == io.EOF {
            break
        }
        if err != nil {
            return totalWritten, fmt.Errorf("%w: read error during compression: %v", 
                daramjwee.ErrCompressionFailed, err)
        }
    }
    
    return totalWritten, nil
}
```

### Algorithm-Specific Optimizations

```go
// Dictionary-based compression
type DictionaryCompressor struct {
    level      int
    dictionary []byte
    encoder    *dictEncoder
}

func NewDictionaryCompressor(level int, dictionary []byte) *DictionaryCompressor {
    return &DictionaryCompressor{
        level:      level,
        dictionary: dictionary,
        encoder:    newDictEncoder(dictionary),
    }
}

func (c *DictionaryCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    // Use pre-trained dictionary for better compression ratios
    c.encoder.Reset(dst)
    return io.Copy(c.encoder, src)
}

// Parallel compression for large data
type ParallelCompressor struct {
    level     int
    workers   int
    chunkSize int
}

func (c *ParallelCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    // Split data into chunks and compress in parallel
    chunks := c.splitIntoChunks(src)
    compressed := make([][]byte, len(chunks))
    
    var wg sync.WaitGroup
    for i, chunk := range chunks {
        wg.Add(1)
        go func(idx int, data []byte) {
            defer wg.Done()
            compressed[idx] = c.compressChunk(data)
        }(i, chunk)
    }
    
    wg.Wait()
    
    // Combine compressed chunks
    return c.combineChunks(dst, compressed)
}
```

## Metadata Handling and Compression Ratio Tracking

### Compression Metadata

```go
// CompressionMetadata tracks compression information
type CompressionMetadata struct {
    Algorithm        string    `json:"algorithm"`
    Level           int       `json:"level"`
    OriginalSize    int64     `json:"original_size"`
    CompressedSize  int64     `json:"compressed_size"`
    CompressionTime time.Duration `json:"compression_time"`
    Checksum        string    `json:"checksum,omitempty"`
}

// Enhanced compressor with metadata tracking
type MetadataTrackingCompressor struct {
    base      daramjwee.Compressor
    algorithm string
    level     int
}

func (c *MetadataTrackingCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    startTime := time.Now()
    
    // Track original size
    originalSize := int64(0)
    compressedSize := int64(0)
    
    // Use TeeReader to count input bytes
    var originalBuf bytes.Buffer
    teeReader := io.TeeReader(src, &originalBuf)
    
    // Perform compression
    written, err := c.base.Compress(dst, teeReader)
    if err != nil {
        return written, err
    }
    
    originalSize = int64(originalBuf.Len())
    compressedSize = written
    compressionTime := time.Since(startTime)
    
    // Store metadata (implementation-specific)
    metadata := &CompressionMetadata{
        Algorithm:       c.algorithm,
        Level:          c.level,
        OriginalSize:   originalSize,
        CompressedSize: compressedSize,
        CompressionTime: compressionTime,
    }
    
    c.storeMetadata(metadata)
    
    return written, nil
}

// Compression ratio calculation
func (m *CompressionMetadata) CompressionRatio() float64 {
    if m.OriginalSize == 0 {
        return 0
    }
    return float64(m.CompressedSize) / float64(m.OriginalSize)
}

func (m *CompressionMetadata) SpaceSavings() float64 {
    return 1.0 - m.CompressionRatio()
}
```

### Checksum Validation

```go
// Compressor with integrity checking
type ChecksumCompressor struct {
    base     daramjwee.Compressor
    hasher   hash.Hash
}

func NewChecksumCompressor(base daramjwee.Compressor) *ChecksumCompressor {
    return &ChecksumCompressor{
        base:   base,
        hasher: sha256.New(),
    }
}

func (c *ChecksumCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    // Calculate checksum while compressing
    c.hasher.Reset()
    teeReader := io.TeeReader(src, c.hasher)
    
    written, err := c.base.Compress(dst, teeReader)
    if err != nil {
        return written, err
    }
    
    // Store checksum for later validation
    checksum := hex.EncodeToString(c.hasher.Sum(nil))
    c.storeChecksum(checksum)
    
    return written, nil
}

func (c *ChecksumCompressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
    // Decompress and validate checksum
    c.hasher.Reset()
    multiWriter := io.MultiWriter(dst, c.hasher)
    
    written, err := c.base.Decompress(multiWriter, src)
    if err != nil {
        return written, err
    }
    
    // Validate checksum
    actualChecksum := hex.EncodeToString(c.hasher.Sum(nil))
    expectedChecksum := c.getStoredChecksum()
    
    if actualChecksum != expectedChecksum {
        return written, fmt.Errorf("%w: checksum mismatch (expected %s, got %s)", 
            daramjwee.ErrCorruptedData, expectedChecksum, actualChecksum)
    }
    
    return written, nil
}
```

## Error Recovery Strategies and Fallback Mechanisms

### Graceful Degradation

```go
// Fallback compressor that tries multiple algorithms
type FallbackCompressor struct {
    primary   daramjwee.Compressor
    fallback  daramjwee.Compressor
    threshold float64  // Compression ratio threshold
}

func (c *FallbackCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    // Try primary compressor first
    var primaryBuf bytes.Buffer
    written, err := c.primary.Compress(&primaryBuf, src)
    
    if err != nil {
        // Primary failed, try fallback
        return c.fallback.Compress(dst, src)
    }
    
    // Check compression effectiveness
    originalSize := c.estimateOriginalSize(src)
    ratio := float64(written) / float64(originalSize)
    
    if ratio > c.threshold {
        // Compression not effective, use fallback (possibly no compression)
        return c.fallback.Compress(dst, src)
    }
    
    // Primary compression was good, use it
    return io.Copy(dst, &primaryBuf)
}

// Retry mechanism for transient failures
type RetryCompressor struct {
    base       daramjwee.Compressor
    maxRetries int
    backoff    time.Duration
}

func (c *RetryCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    var lastErr error
    
    for attempt := 0; attempt <= c.maxRetries; attempt++ {
        if attempt > 0 {
            time.Sleep(c.backoff * time.Duration(attempt))
        }
        
        // Reset source if possible
        if seeker, ok := src.(io.Seeker); ok {
            seeker.Seek(0, io.SeekStart)
        }
        
        written, err := c.base.Compress(dst, src)
        if err == nil {
            return written, nil
        }
        
        lastErr = err
        
        // Don't retry certain types of errors
        if errors.Is(err, daramjwee.ErrInvalidCompressionLevel) ||
           errors.Is(err, daramjwee.ErrUnsupportedAlgorithm) {
            break
        }
    }
    
    return 0, fmt.Errorf("compression failed after %d attempts: %w", 
        c.maxRetries+1, lastErr)
}
```

### Data Recovery

```go
// Self-healing compressor with redundancy
type RedundantCompressor struct {
    primary   daramjwee.Compressor
    secondary daramjwee.Compressor
}

func (c *RedundantCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    // Create redundant compressed data
    var primaryBuf, secondaryBuf bytes.Buffer
    
    // Compress with both algorithms
    teeReader := io.TeeReader(src, &bytes.Buffer{})  // Need to split the stream
    
    _, err1 := c.primary.Compress(&primaryBuf, teeReader)
    _, err2 := c.secondary.Compress(&secondaryBuf, src)
    
    if err1 != nil && err2 != nil {
        return 0, fmt.Errorf("both compression methods failed: primary=%v, secondary=%v", 
            err1, err2)
    }
    
    // Store both versions with metadata indicating which is which
    return c.writeRedundantData(dst, &primaryBuf, &secondaryBuf, err1 == nil, err2 == nil)
}

func (c *RedundantCompressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
    // Try to decompress using available data
    primaryData, secondaryData, primaryValid, secondaryValid := c.readRedundantData(src)
    
    if primaryValid {
        written, err := c.primary.Decompress(dst, primaryData)
        if err == nil {
            return written, nil
        }
    }
    
    if secondaryValid {
        written, err := c.secondary.Decompress(dst, secondaryData)
        if err == nil {
            return written, nil
        }
    }
    
    return 0, fmt.Errorf("%w: both decompression methods failed", 
        daramjwee.ErrDecompressionFailed)
}
```

## Complete Custom Compressor Examples

### Simple Dictionary-Based Compressor

```go
package customcomp

import (
    "bytes"
    "fmt"
    "io"
    "strings"
    
    "github.com/mrchypark/daramjwee"
)

// SimpleDictCompressor implements a basic dictionary-based compression
type SimpleDictCompressor struct {
    level      int
    dictionary map[string]string
    reverse    map[string]string
}

// NewSimpleDictCompressor creates a new dictionary compressor
func NewSimpleDictCompressor(level int, commonWords []string) (*SimpleDictCompressor, error) {
    if level < 1 || level > 10 {
        return nil, daramjwee.ErrInvalidCompressionLevel
    }
    
    dict := make(map[string]string)
    reverse := make(map[string]string)
    
    // Create simple dictionary mapping common words to short codes
    for i, word := range commonWords {
        code := fmt.Sprintf("§%d§", i)  // Use special markers
        dict[word] = code
        reverse[code] = word
    }
    
    return &SimpleDictCompressor{
        level:      level,
        dictionary: dict,
        reverse:    reverse,
    }, nil
}

// Compress replaces dictionary words with shorter codes
func (c *SimpleDictCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    // Read all data (for simplicity - real implementation should stream)
    data, err := io.ReadAll(src)
    if err != nil {
        return 0, fmt.Errorf("%w: failed to read source data: %v", 
            daramjwee.ErrCompressionFailed, err)
    }
    
    text := string(data)
    
    // Replace dictionary words with codes
    for word, code := range c.dictionary {
        text = strings.ReplaceAll(text, word, code)
    }
    
    // Write compressed data
    written, err := dst.Write([]byte(text))
    if err != nil {
        return int64(written), fmt.Errorf("%w: failed to write compressed data: %v", 
            daramjwee.ErrCompressionFailed, err)
    }
    
    return int64(written), nil
}

// Decompress replaces codes with original dictionary words
func (c *SimpleDictCompressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
    // Read compressed data
    data, err := io.ReadAll(src)
    if err != nil {
        return 0, fmt.Errorf("%w: failed to read compressed data: %v", 
            daramjwee.ErrDecompressionFailed, err)
    }
    
    text := string(data)
    
    // Replace codes with original words
    for code, word := range c.reverse {
        text = strings.ReplaceAll(text, code, word)
    }
    
    // Write decompressed data
    written, err := dst.Write([]byte(text))
    if err != nil {
        return int64(written), fmt.Errorf("%w: failed to write decompressed data: %v", 
            daramjwee.ErrDecompressionFailed, err)
    }
    
    return int64(written), nil
}

// Algorithm returns the algorithm name
func (c *SimpleDictCompressor) Algorithm() string {
    return "simple-dict"
}

// Level returns the compression level
func (c *SimpleDictCompressor) Level() int {
    return c.level
}
```

### Advanced Streaming Compressor

```go
package customcomp

import (
    "bufio"
    "fmt"
    "io"
    "sync"
    
    "github.com/mrchypark/daramjwee"
)

// StreamingCompressor implements a streaming compression algorithm
type StreamingCompressor struct {
    level       int
    blockSize   int
    writerPool  sync.Pool
    readerPool  sync.Pool
}

// NewStreamingCompressor creates a new streaming compressor
func NewStreamingCompressor(level int) (*StreamingCompressor, error) {
    if level < 1 || level > 9 {
        return nil, daramjwee.ErrInvalidCompressionLevel
    }
    
    blockSize := 4096 * level  // Larger blocks for higher compression
    
    return &StreamingCompressor{
        level:     level,
        blockSize: blockSize,
        writerPool: sync.Pool{
            New: func() interface{} {
                return &streamingWriter{blockSize: blockSize}
            },
        },
        readerPool: sync.Pool{
            New: func() interface{} {
                return &streamingReader{blockSize: blockSize}
            },
        },
    }, nil
}

// Compress compresses data in streaming fashion
func (c *StreamingCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
    writer := c.writerPool.Get().(*streamingWriter)
    defer c.writerPool.Put(writer)
    
    writer.Reset(dst)
    defer writer.Close()
    
    written, err := io.Copy(writer, src)
    if err != nil {
        return written, fmt.Errorf("%w: streaming compression failed: %v", 
            daramjwee.ErrCompressionFailed, err)
    }
    
    if err := writer.Close(); err != nil {
        return written, fmt.Errorf("%w: failed to finalize compression: %v", 
            daramjwee.ErrCompressionFailed, err)
    }
    
    return written, nil
}

// Decompress decompresses data in streaming fashion
func (c *StreamingCompressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
    reader := c.readerPool.Get().(*streamingReader)
    defer c.readerPool.Put(reader)
    
    reader.Reset(src)
    
    written, err := io.Copy(dst, reader)
    if err != nil {
        return written, fmt.Errorf("%w: streaming decompression failed: %v", 
            daramjwee.ErrDecompressionFailed, err)
    }
    
    return written, nil
}

// Algorithm returns the algorithm name
func (c *StreamingCompressor) Algorithm() string {
    return "streaming"
}

// Level returns the compression level
func (c *StreamingCompressor) Level() int {
    return c.level
}

// streamingWriter implements the compression writer
type streamingWriter struct {
    dst       io.Writer
    blockSize int
    buffer    []byte
    pos       int
}

func (w *streamingWriter) Reset(dst io.Writer) {
    w.dst = dst
    w.buffer = make([]byte, w.blockSize)
    w.pos = 0
}

func (w *streamingWriter) Write(p []byte) (n int, err error) {
    for len(p) > 0 {
        // Fill current block
        space := w.blockSize - w.pos
        toCopy := len(p)
        if toCopy > space {
            toCopy = space
        }
        
        copy(w.buffer[w.pos:], p[:toCopy])
        w.pos += toCopy
        p = p[toCopy:]
        n += toCopy
        
        // Compress and write block if full
        if w.pos == w.blockSize {
            if err := w.flushBlock(); err != nil {
                return n, err
            }
        }
    }
    
    return n, nil
}

func (w *streamingWriter) flushBlock() error {
    if w.pos == 0 {
        return nil
    }
    
    // Simple compression: RLE (Run Length Encoding)
    compressed := w.compressBlock(w.buffer[:w.pos])
    
    // Write block size and compressed data
    if err := w.writeBlockHeader(len(compressed)); err != nil {
        return err
    }
    
    _, err := w.dst.Write(compressed)
    w.pos = 0  // Reset buffer
    return err
}

func (w *streamingWriter) compressBlock(data []byte) []byte {
    // Simple RLE compression
    var result []byte
    if len(data) == 0 {
        return result
    }
    
    current := data[0]
    count := 1
    
    for i := 1; i < len(data); i++ {
        if data[i] == current && count < 255 {
            count++
        } else {
            result = append(result, current, byte(count))
            current = data[i]
            count = 1
        }
    }
    result = append(result, current, byte(count))
    
    return result
}

func (w *streamingWriter) writeBlockHeader(size int) error {
    header := []byte{byte(size >> 8), byte(size & 0xFF)}
    _, err := w.dst.Write(header)
    return err
}

func (w *streamingWriter) Close() error {
    return w.flushBlock()
}

// streamingReader implements the decompression reader
type streamingReader struct {
    src       io.Reader
    blockSize int
    buffer    []byte
    pos       int
    end       int
}

func (r *streamingReader) Reset(src io.Reader) {
    r.src = src
    r.buffer = make([]byte, r.blockSize)
    r.pos = 0
    r.end = 0
}

func (r *streamingReader) Read(p []byte) (n int, err error) {
    for n < len(p) {
        // Fill buffer if empty
        if r.pos >= r.end {
            if err := r.fillBuffer(); err != nil {
                if err == io.EOF && n > 0 {
                    return n, nil
                }
                return n, err
            }
        }
        
        // Copy from buffer
        toCopy := len(p) - n
        available := r.end - r.pos
        if toCopy > available {
            toCopy = available
        }
        
        copy(p[n:], r.buffer[r.pos:r.pos+toCopy])
        r.pos += toCopy
        n += toCopy
    }
    
    return n, nil
}

func (r *streamingReader) fillBuffer() error {
    // Read block header
    header := make([]byte, 2)
    if _, err := io.ReadFull(r.src, header); err != nil {
        return err
    }
    
    blockSize := int(header[0])<<8 | int(header[1])
    if blockSize == 0 {
        return io.EOF
    }
    
    // Read compressed block
    compressed := make([]byte, blockSize)
    if _, err := io.ReadFull(r.src, compressed); err != nil {
        return fmt.Errorf("%w: failed to read compressed block: %v", 
            daramjwee.ErrDecompressionFailed, err)
    }
    
    // Decompress block
    decompressed, err := r.decompressBlock(compressed)
    if err != nil {
        return err
    }
    
    // Store in buffer
    copy(r.buffer, decompressed)
    r.pos = 0
    r.end = len(decompressed)
    
    return nil
}

func (r *streamingReader) decompressBlock(data []byte) ([]byte, error) {
    // Simple RLE decompression
    var result []byte
    
    for i := 0; i < len(data); i += 2 {
        if i+1 >= len(data) {
            return nil, fmt.Errorf("%w: incomplete RLE data", 
                daramjwee.ErrCorruptedData)
        }
        
        value := data[i]
        count := data[i+1]
        
        for j := 0; j < int(count); j++ {
            result = append(result, value)
        }
    }
    
    return result, nil
}
```

## Testing Strategies

### Unit Testing Framework

```go
func TestCustomCompressor_BasicOperations(t *testing.T) {
    compressor := NewCustomCompressor(5)
    
    // Test data
    original := []byte("Hello, World! This is a test string for compression.")
    
    // Compress
    var compressed bytes.Buffer
    written, err := compressor.Compress(&compressed, bytes.NewReader(original))
    assert.NoError(t, err)
    assert.Greater(t, written, int64(0))
    
    // Decompress
    var decompressed bytes.Buffer
    written, err = compressor.Decompress(&decompressed, &compressed)
    assert.NoError(t, err)
    assert.Greater(t, written, int64(0))
    
    // Verify data integrity
    assert.Equal(t, original, decompressed.Bytes())
}

func TestCustomCompressor_ErrorHandling(t *testing.T) {
    // Test invalid compression level
    _, err := NewCustomCompressor(100)
    assert.ErrorIs(t, err, daramjwee.ErrInvalidCompressionLevel)
    
    compressor := NewCustomCompressor(5)
    
    // Test compression with failing writer
    failingWriter := &failingWriter{failAfter: 10}
    _, err = compressor.Compress(failingWriter, strings.NewReader("test data"))
    assert.ErrorIs(t, err, daramjwee.ErrCompressionFailed)
    
    // Test decompression with corrupted data
    var decompressed bytes.Buffer
    corruptedData := bytes.NewReader([]byte("corrupted data"))
    _, err = compressor.Decompress(&decompressed, corruptedData)
    assert.ErrorIs(t, err, daramjwee.ErrDecompressionFailed)
}

func TestCustomCompressor_EdgeCases(t *testing.T) {
    compressor := NewCustomCompressor(5)
    
    // Test empty data
    var compressed, decompressed bytes.Buffer
    _, err := compressor.Compress(&compressed, bytes.NewReader([]byte{}))
    assert.NoError(t, err)
    
    _, err = compressor.Decompress(&decompressed, &compressed)
    assert.NoError(t, err)
    assert.Empty(t, decompressed.Bytes())
    
    // Test large data
    largeData := make([]byte, 1024*1024)  // 1MB
    for i := range largeData {
        largeData[i] = byte(i % 256)
    }
    
    compressed.Reset()
    decompressed.Reset()
    
    _, err = compressor.Compress(&compressed, bytes.NewReader(largeData))
    assert.NoError(t, err)
    
    _, err = compressor.Decompress(&decompressed, &compressed)
    assert.NoError(t, err)
    assert.Equal(t, largeData, decompressed.Bytes())
}
```

### Performance Testing

```go
func BenchmarkCustomCompressor_Compress(b *testing.B) {
    compressor := NewCustomCompressor(5)
    data := make([]byte, 1024)  // 1KB test data
    
    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            var compressed bytes.Buffer
            compressor.Compress(&compressed, bytes.NewReader(data))
        }
    })
}

func BenchmarkCustomCompressor_Decompress(b *testing.B) {
    compressor := NewCustomCompressor(5)
    data := make([]byte, 1024)
    
    // Pre-compress data
    var compressed bytes.Buffer
    compressor.Compress(&compressed, bytes.NewReader(data))
    compressedData := compressed.Bytes()
    
    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            var decompressed bytes.Buffer
            compressor.Decompress(&decompressed, bytes.NewReader(compressedData))
        }
    })
}

func BenchmarkCustomCompressor_CompressionRatio(b *testing.B) {
    compressor := NewCustomCompressor(5)
    
    testCases := []struct {
        name string
        data []byte
    }{
        {"text", []byte(strings.Repeat("Hello World! ", 100))},
        {"binary", make([]byte, 1024)},
        {"random", generateRandomData(1024)},
    }
    
    for _, tc := range testCases {
        b.Run(tc.name, func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                var compressed bytes.Buffer
                written, _ := compressor.Compress(&compressed, bytes.NewReader(tc.data))
                
                ratio := float64(written) / float64(len(tc.data))
                b.ReportMetric(ratio, "compression_ratio")
            }
        })
    }
}
```

### Integration Testing

```go
func TestCustomCompressor_WithDaramjwee(t *testing.T) {
    // Test integration with daramjwee cache
    compressor := NewCustomCompressor(5)
    
    // Create cache with compression
    cache, err := daramjwee.New(
        logger,
        daramjwee.WithHotStore(memstore.New(1024*1024, policy.NewLRU())),
        daramjwee.WithCompression(compressor),
    )
    require.NoError(t, err)
    defer cache.Close()
    
    // Test data storage and retrieval
    testData := "This is test data that should be compressed"
    fetcher := &testFetcher{data: testData}
    
    stream, err := cache.Get(context.Background(), "test-key", fetcher)
    require.NoError(t, err)
    defer stream.Close()
    
    retrieved, err := io.ReadAll(stream)
    require.NoError(t, err)
    assert.Equal(t, testData, string(retrieved))
}
```

## Best Practices

### Algorithm Design

1. **Choose appropriate compression algorithms** based on your data characteristics
2. **Implement streaming operations** to handle large data efficiently
3. **Provide configurable compression levels** for different use cases
4. **Handle edge cases** gracefully (empty data, corrupted data)
5. **Optimize for your specific use case** (speed vs. compression ratio)

### Performance Optimization

1. **Use buffer pooling** to reduce memory allocations
2. **Implement proper streaming** to avoid loading large data into memory
3. **Consider parallel compression** for large data sets
4. **Profile your implementation** under realistic workloads
5. **Optimize for common data patterns** in your application

### Error Handling

1. **Use standard daramjwee error types** for consistency
2. **Provide detailed error messages** for debugging
3. **Implement proper resource cleanup** on errors
4. **Validate input parameters** and return appropriate errors
5. **Handle data corruption** gracefully with proper detection

### Code Quality

1. **Document algorithm behavior** and performance characteristics
2. **Provide comprehensive tests** including edge cases and error conditions
3. **Follow Go idioms** and conventions
4. **Implement proper resource management** (close readers/writers)
5. **Consider thread safety** if your compressor maintains state

## Common Pitfalls

1. **Not handling streaming data** properly (loading everything into memory)
2. **Poor error handling** that doesn't use standard error types
3. **Memory leaks** from not closing resources properly
4. **Not validating compression levels** during construction
5. **Ignoring data corruption** during decompression
6. **Poor performance** due to excessive memory allocations
7. **Not testing with realistic data** sizes and patterns
8. **Complex algorithms** that don't provide significant benefits

This comprehensive guide should help you implement robust, efficient, and well-tested custom compression algorithms for the daramjwee caching library.