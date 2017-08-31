package packet

import (
	"encoding/binary"
)

// WriteToBuffer encode and send message into ring buffer
//func WriteToBuffer(msg Provider, to *goring.Buffer) (int, error) {
//	expectedSize, err := msg.Size()
//	if err != nil {
//		return 0, err
//	}
//
//	if len(to.ExternalBuf) < expectedSize {
//		to.ExternalBuf = make([]byte, expectedSize)
//	}
//
//	total, err := msg.Encode(to.ExternalBuf)
//	if err != nil {
//		return 0, err
//	}
//
//	return to.Send([][]byte{to.ExternalBuf[:total]})
//}

// ReadLPBytes read length prefixed bytes
func ReadLPBytes(buf []byte) ([]byte, int, error) {
	if len(buf) < 2 {
		return nil, 0, ErrInsufficientDataSize
	}

	var n int
	total := 0

	// Read length prefix
	n = int(binary.BigEndian.Uint16(buf))
	total += 2

	// Check for malformed length-prefixed field
	// if remaining space is less than length-prefixed size the packet seems to be broken
	if len(buf[total:]) < n {
		return nil, total, ErrInsufficientDataSize
	}

	total += n

	return buf[2:total], total, nil
}

// WriteLPBytes write length prefixed bytes
func WriteLPBytes(buf []byte, b []byte) (int, error) {
	total, n := 0, len(b)

	if n > MaxLPString {
		return 0, ErrInvalidLPStringSize
	}

	if len(buf) < 2+n {
		return 2 + n, ErrInsufficientBufferSize
	}

	binary.BigEndian.PutUint16(buf, uint16(n))
	total += 2

	copy(buf[total:], b)
	total += n

	return total, nil
}
