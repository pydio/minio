package cmd

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/pkg/auth"
)

// s3UnsignedChunkedReader reads and parses an S3 chunked stream using "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
type s3UnsignedChunkedReader struct {
	reader        *bufio.Reader
	cred          auth.Credentials
	seedSignature string
	seedDate      time.Time
	region        string

	currBuf []byte
	eof     bool

	readSize  int64    // Track total bytes read for validation
	signature []string // Store chunk signatures if needed

}

// newUnsignedV4ChunkedReader creates a new AWS S3 chunked reader
func newUnsignedV4ChunkedReader(req *http.Request) (io.ReadCloser, APIErrorCode) {
	cred, seedSignature, region, seedDate, errCode := calculateSeedSignature(req)
	if errCode != ErrNone {
		return nil, errCode
	}

	return &s3UnsignedChunkedReader{
		reader:        bufio.NewReader(req.Body),
		cred:          cred,
		seedSignature: seedSignature,
		seedDate:      seedDate,
		region:        region,
	}, ErrNone
}

func (r *s3UnsignedChunkedReader) Close() (err error) {
	return nil
}

// Read reads from the AWS chunked stream
func (r *s3UnsignedChunkedReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// If we have leftover buffer data, return from there first
	if len(r.currBuf) > 0 {
		n = copy(p, r.currBuf)
		r.currBuf = r.currBuf[n:]
		r.readSize += int64(n)
		return n, nil
	}

	// If EOF, return consistently
	if r.eof {
		return 0, io.EOF
	}

	// Read next chunk
	size, signature, err := r.readChunkHeader()
	if err != nil {
		return 0, fmt.Errorf("read chunk header: %w", err)
	}

	// Store signature if present
	if signature != "" {
		r.signature = append(r.signature, signature)
	}

	// If last chunk (0 size), handle trailer and exit
	if size == 0 {
		if err := r.readFinalTrailer(); err != nil {
			return 0, fmt.Errorf("read final trailer: %w", err)
		}
		r.eof = true
		return 0, io.EOF
	}

	// Read chunk data with bounds checking
	if size < 0 {
		return 0, fmt.Errorf("%w: negative chunk size", errMalformedEncoding)
	}

	data := make([]byte, size)
	_, err = io.ReadFull(r.reader, data)
	if err != nil {
		return 0, fmt.Errorf("read chunk data: %w", err)
	}

	// Read trailing \r\n
	c1, err := r.reader.ReadByte()
	if err != nil || c1 != '\r' {
		return 0, fmt.Errorf("%w: missing chunk delimiter", errMalformedEncoding)
	}

	c2, err := r.reader.ReadByte()
	if err != nil || c2 != '\n' {
		return 0, fmt.Errorf("%w: missing chunk delimiter", errMalformedEncoding)
	}

	// Store data in buffer
	r.currBuf = data

	// Return from buffer
	n = copy(p, r.currBuf)
	r.currBuf = r.currBuf[n:]
	r.readSize += int64(n)

	return n, nil
}

// readChunkHeader reads the AWS chunk header
func (r *s3UnsignedChunkedReader) readChunkHeader() (size int, signature string, err error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return 0, "", err
	}
	line = strings.TrimSpace(line)

	// Validate minimum length
	if len(line) == 0 {
		return 0, "", fmt.Errorf("%w: empty chunk header", errMalformedEncoding)
	}

	// Extract chunk size and optional signature
	parts := strings.Split(line, ";")
	sizeInt, err := strconv.ParseInt(parts[0], 16, 64)
	if err != nil {
		return 0, "", fmt.Errorf("%w: invalid chunk size", errMalformedEncoding)
	}

	// Extract signature if available
	if len(parts) > 1 {
		if !strings.HasPrefix(parts[1], "chunk-signature=") {
			return 0, "", fmt.Errorf("%w: invalid signature format", errMalformedEncoding)
		}
		signature = strings.TrimPrefix(parts[1], "chunk-signature=")
	}

	return int(sizeInt), signature, nil
}

// readFinalTrailer reads the final signature trailer
func (r *s3UnsignedChunkedReader) readFinalTrailer() error {
	foundContentSha := false

	// AWS streaming sends trailers after the last chunk
	for {
		line, err := r.reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read trailer line: %w", err)
		}
		line = strings.TrimSpace(line)

		// End of headers (empty line) means trailer section is done
		if line == "" {
			break
		}

		// Handle required x-amz-content-sha256 trailer
		if strings.HasPrefix(line, "x-amz-content-sha256:") {
			trailerHash := strings.TrimSpace(strings.TrimPrefix(line, "x-amz-content-sha256:"))
			if trailerHash == "" {
				return fmt.Errorf("%w: empty content hash", errMalformedEncoding)
			}
			foundContentSha = true
		}
	}

	if !foundContentSha {
		return errMalformedEncoding
	}

	return nil
}

// GetTotalBytesRead returns the total number of payload bytes read
func (r *s3UnsignedChunkedReader) GetTotalBytesRead() int64 {
	return r.readSize
}

// GetChunkSignatures returns the collected chunk signatures if available
func (r *s3UnsignedChunkedReader) GetChunkSignatures() []string {
	return append([]string{}, r.signature...)
}
