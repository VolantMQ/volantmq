package packet

import "encoding/binary"

type sizeCallback func() int
type encodeCallback func([]byte) (int, error)
type decodeCallback func([]byte) (int, error)

type header struct {
	cb struct {
		encode encodeCallback
		decode decodeCallback
		size   sizeCallback
	}

	properties property
	packetID   []byte
	remLen     int32
	mFlags     byte
	mType      Type
	version    ProtocolVersion
}

const (
	offsetPacketType byte = 0x04
	// offsetPublishFlagRetain     byte = 0x00
	offsetPublishFlagQoS byte = 0x01
	// offsetPublishFlagDup        byte = 0x03
	offsetConnFlagWillQoS byte = 0x03
	// offsetSubscribeOps             byte = 0x06
	// offsetSubscriptionQoS          byte = 0x00
	offsetSubscriptionNL             byte = 0x02
	offsetSubscriptionRAP            byte = 0x03
	offsetSubscriptionRetainHandling byte = 0x04
	// offsetSubscriptionReserved     byte = 0x06

)

const (
	maskMessageFlags               byte = 0x0F
	maskConnFlagUsername           byte = 0x80
	maskConnFlagPassword           byte = 0x40
	maskConnFlagWillRetain         byte = 0x20
	maskConnFlagWillQos            byte = 0x18
	maskConnFlagWill               byte = 0x04
	maskConnFlagClean              byte = 0x02
	maskConnFlagReserved           byte = 0x01
	maskPublishFlagRetain          byte = 0x01
	maskPublishFlagQoS             byte = 0x06
	maskPublishFlagDup             byte = 0x08
	maskSubscriptionQoS            byte = 0x03
	maskSubscriptionNL             byte = 0x04
	maskSubscriptionRAP            byte = 0x08
	maskSubscriptionRetainHandling byte = 0x30
	maskSubscriptionReserved       byte = 0xC0
)

func (h *header) init(t Type, v ProtocolVersion, sz func() int, enc, dec func([]byte) (int, error)) {
	h.mType = t
	h.version = v
	h.cb.encode = enc
	h.cb.decode = dec
	h.cb.size = sz

	if v >= ProtocolV50 {
		h.properties.reset()
	}
}

// Name returns a string representation of the message type. Examples include
// "PUBLISH", "SUBSCRIBE", and others. This is statically defined for each of
// the message types and cannot be changed.
func (h *header) Name() string {
	return h.Type().Name()
}

// Desc returns a string description of the message type. For example, a
// CONNECT message would return "Client request to connect to Server." These
// descriptions are statically defined (copied from the MQTT spec) and cannot
// be changed.
func (h *header) Desc() string {
	return h.Type().Desc()
}

// Type returns the MessageType of the Message
func (h *header) Type() Type {
	return h.mType
}

// Flags returns the fixed header flags for this message.
func (h *header) Flags() byte {
	return h.mFlags
}

// RemainingLength returns the length of the non-fixed-header part of the message.
func (h *header) RemainingLength() int32 {
	return h.remLen
}

// Version protocol version used by packet
func (h *header) Version() ProtocolVersion {
	return h.version
}

// ID packet id, valid only for
// PUBLISH (QoS 1/2), PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK
func (h *header) ID() (IDType, error) {
	if len(h.packetID) == 0 {
		return 0, ErrNotSet
	}

	return IDType(binary.BigEndian.Uint16(h.packetID)), nil
}

// Encode packet into buffer, Size() should be called to determine expected buffer size
func (h *header) Encode(to []byte) (int, error) {
	expectedSize, err := h.Size()
	if err != nil {
		return 0, err
	}

	if expectedSize > len(to) {
		return expectedSize, ErrInsufficientBufferSize
	}

	offset := 0

	to[offset] = byte(h.mType<<offsetPacketType) | h.mFlags
	offset++

	offset += binary.PutUvarint(to[offset:], uint64(h.remLen))

	var n int

	n, err = h.cb.encode(to[offset:])
	offset += n
	return offset, err
}

// SetVersion protocol version used to encode packet
func (h *header) SetVersion(v ProtocolVersion) {
	h.version = v
}

// Size of message
func (h *header) Size() (int, error) {
	ml := h.cb.size()

	if err := h.setRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	return h.size() + ml, nil
}

// PropertiesDiscard discard all previously set properties
func (h *header) PropertiesDiscard() {
	h.properties.reset()
}

// PropertyGet get property value, nil if not present
func (h *header) PropertyGet(id PropertyID) PropertyToType {
	if h.version != ProtocolV50 {
		return nil
	}

	return h.properties.Get(id)
}

// PropertySet set value
func (h *header) PropertySet(id PropertyID, val interface{}) error {
	if h.version != ProtocolV50 {
		return ErrNotSupported
	}

	return h.properties.Set(h.mType, id, val)
}

// PropertyForEach iterate over properties
func (h *header) PropertyForEach(f func(PropertyID, PropertyToType)) error {
	if h.version != ProtocolV50 {
		return ErrNotSupported
	}

	h.properties.ForEach(f)

	return nil
}

func (h *header) setPacketID(id IDType) {
	if len(h.packetID) == 0 {
		h.packetID = make([]byte, 2)
	}
	binary.BigEndian.PutUint16(h.packetID, uint16(id))
}

func (h *header) decodePacketID(src []byte) int {
	if len(h.packetID) == 0 {
		h.packetID = make([]byte, 2)
	}

	return copy(h.packetID, src)
}

func (h *header) encodePacketID(dst []byte) int {
	return copy(dst, h.packetID)
}

// setRemainingLength sets the length of the non-fixed-header part of the message.
// It returns error if the length is greater than 268435455, which is the max
// message length as defined by the MQTT spec.
func (h *header) setRemainingLength(remLen int32) error {
	if remLen > maxRemainingLength || remLen < 0 {
		return ErrInvalidLength
	}

	h.remLen = remLen

	return nil
}

func (h *header) getHeader() *header {
	return h
}

// size of header
// this function must be invoked after successful call to setRemainingLength
func (h *header) size() int {
	// message type and flags byte
	total := 1

	return total + uvarintCalc(uint32(h.remLen))
}

// setType sets the message type of this message. It also correctly sets the
// default flags for the message type. It returns an error if the type is invalid.
func (h *header) setType(t Type) {
	// Notice we don't set the message to be dirty when we are not allocating a new
	// buffer. In this case, it means the buffer is probably a sub-slice of another
	// slice. If that's the case, then during encoding we would have copied the whole
	// backing buffer anyway.
	h.mType = t
	h.mFlags = t.DefaultFlags()
}

// decode reads fixed header and remaining length
// if decode successful size of decoded data provided
// if error happened offset points to error place
func (h *header) decode(from []byte) (int, error) {
	offset := 0

	// decode and validate fixed header
	h.mType = Type(from[offset] >> offsetPacketType)
	h.mFlags = from[offset] & maskMessageFlags

	reject := false
	// [MQTT-2.2.2-1]
	if h.mType != PUBLISH && h.mFlags != h.mType.DefaultFlags() {
		reject = true
	} else {
		if !QosType((h.mFlags & maskPublishFlagQoS) >> offsetPublishFlagQoS).IsValid() {
			reject = true
		}
	}

	if reject {
		rejectCode := CodeRefusedServerUnavailable
		if h.version == ProtocolV50 {
			rejectCode = CodeMalformedPacket
		}
		return offset, rejectCode
	}

	offset++

	remLen, m := uvarint(from[offset:])
	if m <= 0 {
		return offset, ErrInsufficientDataSize
	}

	offset += m
	h.remLen = int32(remLen)

	// verify if buffer has enough space for whole message
	// if not return expected size
	if int(h.remLen) > len(from[offset:]) {
		return offset + int(h.remLen), ErrInsufficientDataSize
	}

	var err error
	if h.cb.decode != nil {
		var msgTotal int

		msgTotal, err = h.cb.decode(from[offset:])
		offset += msgTotal
	}
	return offset, err
}

// uvarint decodes a uint32 from buf and returns that value and the
// number of bytes read (> 0). If an error occurred, the value is 0
// and the number of bytes n is <= 0 meaning:
//
//	n == 0: buf too small
//	n  < 0: value larger than 32 bits (overflow)
//              and -n is the number of bytes read
//
// copied from binary.Uvariant
func uvarint(buf []byte) (uint32, int) {
	var x uint32
	var s uint
	for i, b := range buf {
		if b < 0x80 {
			if i > 4 || i == 4 && b > 1 {
				return 0, -(i + 1) // overflow
			}
			return x | uint32(b)<<s, i + 1
		}
		x |= uint32(b&0x7f) << s
		s += 7
	}
	return 0, 0
}

func uvarintCalc(x uint32) int {
	i := 0
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}
