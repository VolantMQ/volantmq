package packet

// Error errors
type Error byte

// nolint: golint
const (
	// ErrInvalidUnSubscribe Invalid UNSUBSCRIBE message
	ErrInvalidUnSubscribe Error = iota
	// ErrInvalidUnSubAck Invalid UNSUBACK message
	ErrInvalidUnSubAck
	ErrDupViolation
	// ErrPackedIDNotMatched Packet ID does not match
	ErrPackedIDNotMatched
	ErrInvalid
	// ErrPackedIDZero cannot be 0
	ErrPackedIDZero
	// ErrOnPublishNil Publisher is nil
	ErrOnPublishNil
	// ErrInvalidMessageType Invalid message type
	ErrInvalidMessageType
	// ErrInvalidMessageTypeFlags Invalid message flags
	ErrInvalidMessageTypeFlags
	// ErrInvalidQoS Invalid message QoS
	ErrInvalidQoS
	// ErrInvalidLength Invalid message length
	ErrInvalidLength
	// ErrProtocolViolation Message Will violation
	ErrProtocolViolation
	// ErrInsufficientBufferSize Insufficient buffer size
	ErrInsufficientBufferSize
	// ErrInsufficientDataSize
	ErrInsufficientDataSize
	// ErrInvalidTopic Topic is empty
	ErrInvalidTopic
	// ErrEmptyPayload Payload is empty
	ErrEmptyPayload
	// ErrInvalidReturnCode invalid return code
	ErrInvalidReturnCode
	// ErrUnimplemented method not implemented
	ErrUnimplemented
	// ErrInvalidLPStringSize LP string size is bigger than expected
	ErrInvalidLPStringSize
	// ErrMalformedTopic topic string is not UTF8
	ErrMalformedTopic
	ErrMalformedStream
	ErrInvalidProtocolVersion
	ErrNotSet
	ErrPanicDetected
	ErrInvalidArgs
	ErrInvalidUtf8
	ErrNotSupported
	ErrProtocolInvalidName
)

// Error returns the corresponding error string for the ConnAckCode
func (e Error) Error() string {
	switch e {
	case ErrInvalidUnSubscribe:
		return "Invalid UNSUBSCRIBE message"
	case ErrInvalidUnSubAck:
		return "Invalid UNSUBACK message"
	case ErrDupViolation:
		return "Duplicate violation"
	case ErrPackedIDNotMatched:
		return "Packet ID does not match"
	case ErrPackedIDZero:
		return "Packet ID cannot be 0"
	case ErrOnPublishNil:
		return "Publisher is nil"
	case ErrInvalidMessageType:
		return "Invalid message type"
	case ErrInvalidMessageTypeFlags:
		return "Invalid message flags"
	case ErrInvalidQoS:
		return "Invalid message QoS"
	case ErrInvalidLength:
		return "Invalid message length"
	case ErrProtocolViolation:
		return "Protocol violation"
	case ErrInsufficientBufferSize:
		return "Insufficient buffer size"
	case ErrInsufficientDataSize:
		return "Insufficient data size"
	case ErrInvalidTopic:
		return "Invalid topic name"
	case ErrEmptyPayload:
		return "Payload is empty"
	case ErrInvalidReturnCode:
		return "Invalid return code"
	case ErrUnimplemented:
		return "Function not implemented yet"
	case ErrInvalidLPStringSize:
		return "Invalid LP string size"
	case ErrMalformedTopic:
		return "Malformed topic"
	case ErrMalformedStream:
		return "Malformed stream"
	case ErrInvalidArgs:
		return "Invalid arguments"
	case ErrInvalidUtf8:
		return "String is not UTF8"
	case ErrInvalidProtocolVersion:
		return "Invalid protocol name"
	}

	return "Unknown error"
}
