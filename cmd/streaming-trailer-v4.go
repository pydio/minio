package cmd

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"
)

// readAndValidateFinalTrailer reads the final signature trailer,
// If hash.Hash is passed, compares it to the trailer value.
// Otherwise it just validates that it is non-empty.
func readAndValidateFinalTrailer(reader *bufio.Reader, compareTo hash.Hash) error {
	foundContentSha := false

	// AWS streaming sends trailers after the last chunk
	for {
		line, err := reader.ReadString('\n')
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
			if compareTo != nil {
				final := hex.EncodeToString(compareTo.Sum(nil))
				if !strings.EqualFold(trailerHash, final) {
					return fmt.Errorf("%w: final hash mismatch: expected %s, got %s", errMalformedEncoding, final, trailerHash)
				}
			}
			foundContentSha = true
		}
	}

	if !foundContentSha {
		return errMalformedEncoding
	}

	return nil
}
