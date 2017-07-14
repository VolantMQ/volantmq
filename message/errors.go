package message

// Error errors
type Error byte

const (
	// ErrInvalidUnSubscribe Invalid UNSUBSCRIBE message
	ErrInvalidUnSubscribe Error = iota
	// ErrInvalidUnSubAck Invalid UNSUBACK message
	ErrInvalidUnSubAck
	// ErrPackedIDNotMatched Packet ID does not match
	ErrPackedIDNotMatched
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
)

// Error returns the corresponding error string for the ConnAckCode
func (e Error) Error() string {
	switch e {
	case ErrInvalidUnSubscribe:
		return "Invalid UNSUBSCRIBE message"
	case ErrInvalidUnSubAck:
		return "Invalid UNSUBACK message"
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
	}

	return "Unknown error"
}
