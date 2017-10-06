package packet

import (
	"encoding/binary"
	"unicode/utf8"
)

// PropertyID id as per [MQTT-2.2.2]
type PropertyID uint32

// PropertyType value type used be property ID
type PropertyType byte

// PropertyError encodes property error
type PropertyError int

// nolint: golint
const (
	ErrPropertyNotFound PropertyError = iota
	ErrPropertyInvalidID
	ErrPropertyPacketTypeMismatch
	ErrPropertyTypeMismatch
	ErrPropertyDuplicate
	ErrPropertyUnsupported
	ErrPropertyWrongType
)

// Error description
func (e PropertyError) Error() string {
	switch e {
	case ErrPropertyNotFound:
		return "property: id not found"
	case ErrPropertyInvalidID:
		return "property: id is invalid"
	case ErrPropertyPacketTypeMismatch:
		return "property: packet type does not match id"
	case ErrPropertyTypeMismatch:
		return "property: value type does not match id"
	case ErrPropertyDuplicate:
		return "property: duplicate of id not allowed"
	case ErrPropertyUnsupported:
		return "property: value type is unsupported"
	case ErrPropertyWrongType:
		return "property: value type differs from expected"
	default:
		return "property: unknown error"
	}
}

// StringPair user defined properties
type StringPair struct {
	K string
	V string
}

// PropertyToType represent property value as requested type
type PropertyToType interface {
	Type() PropertyType
	AsByte() (byte, error)
	AsShort() (uint16, error)
	AsInt() (uint32, error)
	AsString() (string, error)
	AsStringPair() (StringPair, error)
	AsStringPairs() ([]StringPair, error)
	AsBinary() ([]byte, error)
}

type propertyToType struct {
	t PropertyType
	v interface{}
}

var _ PropertyToType = (*propertyToType)(nil)

func (t *propertyToType) Type() PropertyType {
	return t.t
}

func (t *propertyToType) AsByte() (byte, error) {
	if v, ok := t.v.(byte); ok {
		return v, nil
	}

	return 0, ErrPropertyWrongType
}

func (t *propertyToType) AsShort() (uint16, error) {
	if v, ok := t.v.(uint16); ok {
		return v, nil
	}

	return 0, ErrPropertyWrongType
}

func (t *propertyToType) AsInt() (uint32, error) {
	if v, ok := t.v.(uint32); ok {
		return v, nil
	}

	return 0, ErrPropertyWrongType
}

func (t *propertyToType) AsString() (string, error) {
	if v, ok := t.v.(string); ok {
		return v, nil
	}

	return "", ErrPropertyWrongType
}

func (t *propertyToType) AsStringPair() (StringPair, error) {
	if v, ok := t.v.(StringPair); ok {
		return v, nil
	}

	return StringPair{}, ErrPropertyWrongType
}

func (t *propertyToType) AsStringPairs() ([]StringPair, error) {
	if v, ok := t.v.([]StringPair); ok {
		return v, nil
	}

	return []StringPair{}, ErrPropertyWrongType
}

func (t *propertyToType) AsBinary() ([]byte, error) {
	if v, ok := t.v.([]byte); ok {
		return v, nil
	}

	return []byte{}, ErrPropertyWrongType
}

// property implements Property
type property struct {
	properties map[PropertyID]interface{}
	len        uint32
}

// nolint: golint
const (
	PropertyPayloadFormat                   = PropertyID(0x01)
	PropertyPublicationExpiry               = PropertyID(0x02)
	PropertyContentType                     = PropertyID(0x03)
	PropertyResponseTopic                   = PropertyID(0x08)
	PropertyCorrelationData                 = PropertyID(0x09)
	PropertySubscriptionIdentifier          = PropertyID(0x0B)
	PropertySessionExpiryInterval           = PropertyID(0x11)
	PropertyAssignedClientIdentifier        = PropertyID(0x12)
	PropertyServerKeepAlive                 = PropertyID(0x13)
	PropertyAuthMethod                      = PropertyID(0x15)
	PropertyAuthData                        = PropertyID(0x16)
	PropertyRequestProblemInfo              = PropertyID(0x17)
	PropertyWillDelayInterval               = PropertyID(0x18)
	PropertyRequestResponseInfo             = PropertyID(0x19)
	PropertyResponseInfo                    = PropertyID(0x1A)
	PropertyServerReverence                 = PropertyID(0x1C)
	PropertyReasonString                    = PropertyID(0x1F)
	PropertyReceiveMaximum                  = PropertyID(0x21)
	PropertyTopicAliasMaximum               = PropertyID(0x22)
	PropertyTopicAlias                      = PropertyID(0x23)
	PropertyMaximumQoS                      = PropertyID(0x24)
	PropertyRetainAvailable                 = PropertyID(0x25)
	PropertyUserProperty                    = PropertyID(0x26)
	PropertyMaximumPacketSize               = PropertyID(0x27)
	PropertyWildcardSubscriptionAvailable   = PropertyID(0x28)
	PropertySubscriptionIdentifierAvailable = PropertyID(0x29)
	PropertySharedSubscriptionAvailable     = PropertyID(0x2A)
)

// nolint: golint
const (
	PropertyTypeByte = iota
	PropertyTypeShort
	PropertyTypeInt
	PropertyTypeVarInt
	PropertyTypeString
	PropertyTypeStringPair
	PropertyTypeBinary
)

var propertyAllowedMessageTypes = map[PropertyID]map[Type]bool{
	PropertyPayloadFormat:                   {PUBLISH: false},
	PropertyPublicationExpiry:               {PUBLISH: false},
	PropertyContentType:                     {PUBLISH: false},
	PropertyResponseTopic:                   {PUBLISH: false},
	PropertyCorrelationData:                 {PUBLISH: false},
	PropertySubscriptionIdentifier:          {PUBLISH: true, SUBSCRIBE: false},
	PropertySessionExpiryInterval:           {CONNECT: false, DISCONNECT: false},
	PropertyAssignedClientIdentifier:        {CONNACK: false},
	PropertyServerKeepAlive:                 {CONNACK: false},
	PropertyAuthMethod:                      {CONNECT: false, CONNACK: false, AUTH: false},
	PropertyAuthData:                        {CONNECT: false, CONNACK: false, AUTH: false},
	PropertyWillDelayInterval:               {CONNECT: false},
	PropertyRequestProblemInfo:              {CONNECT: false},
	PropertyRequestResponseInfo:             {CONNECT: false},
	PropertyResponseInfo:                    {CONNACK: false},
	PropertyServerReverence:                 {CONNACK: false, DISCONNECT: false},
	PropertyReceiveMaximum:                  {CONNECT: false, CONNACK: false},
	PropertyTopicAliasMaximum:               {CONNECT: false, CONNACK: false},
	PropertyTopicAlias:                      {PUBLISH: false},
	PropertyMaximumQoS:                      {CONNACK: false},
	PropertyRetainAvailable:                 {CONNACK: false},
	PropertyMaximumPacketSize:               {CONNECT: false, CONNACK: false},
	PropertyWildcardSubscriptionAvailable:   {CONNACK: false},
	PropertySubscriptionIdentifierAvailable: {CONNACK: false},
	PropertySharedSubscriptionAvailable:     {CONNACK: false},
	PropertyReasonString: {
		CONNACK:    false,
		PUBACK:     false,
		PUBREC:     false,
		PUBREL:     false,
		PUBCOMP:    false,
		SUBACK:     false,
		UNSUBACK:   false,
		DISCONNECT: false,
		AUTH:       false},
	PropertyUserProperty: {
		CONNECT:    true,
		CONNACK:    true,
		PUBLISH:    true,
		PUBACK:     true,
		PUBREC:     true,
		PUBREL:     true,
		PUBCOMP:    true,
		SUBACK:     true,
		UNSUBACK:   true,
		DISCONNECT: true,
		AUTH:       true},
}

var propertyTypeMap = map[PropertyID]PropertyType{
	PropertyPayloadFormat:                   PropertyTypeByte,
	PropertyPublicationExpiry:               PropertyTypeInt,
	PropertyContentType:                     PropertyTypeString,
	PropertyResponseTopic:                   PropertyTypeString,
	PropertyCorrelationData:                 PropertyTypeBinary,
	PropertySubscriptionIdentifier:          PropertyTypeVarInt,
	PropertySessionExpiryInterval:           PropertyTypeInt,
	PropertyAssignedClientIdentifier:        PropertyTypeString,
	PropertyServerKeepAlive:                 PropertyTypeShort,
	PropertyAuthMethod:                      PropertyTypeString,
	PropertyAuthData:                        PropertyTypeBinary,
	PropertyRequestProblemInfo:              PropertyTypeByte,
	PropertyWillDelayInterval:               PropertyTypeInt,
	PropertyRequestResponseInfo:             PropertyTypeByte,
	PropertyResponseInfo:                    PropertyTypeString,
	PropertyServerReverence:                 PropertyTypeString,
	PropertyReasonString:                    PropertyTypeString,
	PropertyReceiveMaximum:                  PropertyTypeShort,
	PropertyTopicAliasMaximum:               PropertyTypeShort,
	PropertyTopicAlias:                      PropertyTypeShort,
	PropertyMaximumQoS:                      PropertyTypeByte,
	PropertyRetainAvailable:                 PropertyTypeByte,
	PropertyUserProperty:                    PropertyTypeStringPair,
	PropertyMaximumPacketSize:               PropertyTypeInt,
	PropertyWildcardSubscriptionAvailable:   PropertyTypeByte,
	PropertySubscriptionIdentifierAvailable: PropertyTypeByte,
	PropertySharedSubscriptionAvailable:     PropertyTypeByte,
}

var propertyDecodeType = map[PropertyType]func(*property, PropertyID, []byte) (int, error){
	PropertyTypeByte:       decodeByte,
	PropertyTypeShort:      decodeShort,
	PropertyTypeInt:        decodeInt,
	PropertyTypeVarInt:     decodeVarInt,
	PropertyTypeString:     decodeString,
	PropertyTypeStringPair: decodeStringPair,
	PropertyTypeBinary:     decodeBinary,
}

var propertyEncodeType = map[PropertyType]func(id PropertyID, val interface{}, to []byte) (int, error){
	PropertyTypeByte:       encodeByte,
	PropertyTypeShort:      encodeShort,
	PropertyTypeInt:        encodeInt,
	PropertyTypeVarInt:     encodeVarInt,
	PropertyTypeString:     encodeString,
	PropertyTypeStringPair: encodeStringPair,
	PropertyTypeBinary:     encodeBinary,
}

var propertyCalcLen = map[PropertyType]func(id PropertyID, val interface{}) (int, error){
	PropertyTypeByte:       calcLenByte,
	PropertyTypeShort:      calcLenShort,
	PropertyTypeInt:        calcLenInt,
	PropertyTypeVarInt:     calcLenVarInt,
	PropertyTypeString:     calcLenString,
	PropertyTypeStringPair: calcLenStringPair,
	PropertyTypeBinary:     calcLenBinary,
}

// DupAllowed check if property id allows keys duplication
func (p PropertyID) DupAllowed(t Type) bool {
	d, ok := propertyAllowedMessageTypes[p]
	if !ok {
		return false
	}

	return d[t]
}

// IsValid check if property id is valid spec value
func (p PropertyID) IsValid() bool {
	if _, ok := propertyTypeMap[p]; ok {
		return true
	}

	return false
}

// IsValidPacketType check either property id can be used for given packet type
func (p PropertyID) IsValidPacketType(t Type) bool {
	mT, ok := propertyAllowedMessageTypes[p]
	if !ok {
		return false
	}

	if _, ok = mT[t]; !ok {
		return false
	}

	return true
}

func (p *property) reset() {
	p.properties = make(map[PropertyID]interface{})
	p.len = 0
}

// Len of the encoded property field. Does not include size property len prefix
func (p *property) Len() (uint32, int) {
	return p.len, uvarintCalc(p.len)
}

// FullLen len of the property len prefix + size of properties
func (p *property) FullLen() uint32 {
	return p.len + uint32(uvarintCalc(p.len))
}

// Set property value
func (p *property) Set(t Type, id PropertyID, val interface{}) error {
	if mT, ok := propertyAllowedMessageTypes[id]; !ok {
		return ErrPropertyInvalidID
	} else if _, ok = mT[t]; !ok {
		return ErrPropertyPacketTypeMismatch
	}

	fn := propertyCalcLen[propertyTypeMap[id]]
	l, _ := fn(id, val)
	p.len += uint32(l)
	p.properties[id] = val

	return nil
}

// Get property value
func (p *property) Get(id PropertyID) PropertyToType {
	if val, ok := p.properties[id]; ok {
		t := propertyTypeMap[id]
		return &propertyToType{v: val, t: t}
	}

	return nil
}

// ForEach iterate over existing properties
func (p *property) ForEach(f func(PropertyID, PropertyToType)) {
	if p.properties == nil {
		return
	}

	for k, v := range p.properties {
		t := propertyTypeMap[k]
		f(k, &propertyToType{v: v, t: t})
	}
}

func writePrefixID(id PropertyID, b []byte) int {
	return binary.PutUvarint(b, uint64(id))
}

//func decodeProperties(t Type, buf []byte) (*property, int, error) {
//	p := newProperty()
//
//	total, err := p.decode(t, buf)
//	if err != nil {
//		return nil, total, err
//	}
//
//	return p, total, nil
//}

//func encodeProperties(p *property, dst []byte) (int, error) {
//	if p == nil {
//		if len(dst) > 0 {
//			dst[0] = 0
//		}
//		return 1, nil
//	}
//
//	return p.encode(dst)
//}

func (p *property) decode(t Type, from []byte) (int, error) {
	offset := 0
	// property length is encoded as variable byte integer
	pLen, lCount := uvarint(from)
	if lCount <= 0 {
		offset += -lCount
		return offset, CodeMalformedPacket
	}

	offset += lCount

	var err error

	for pLen != 0 {
		slice := from[offset:]

		idVal, count := uvarint(slice)
		if count <= 0 {
			return offset - count, CodeMalformedPacket
		}

		id := PropertyID(idVal)

		if !id.IsValidPacketType(t) {
			return offset, CodeMalformedPacket
		}

		if _, ok := p.properties[id]; ok && !id.DupAllowed(t) {
			return offset, CodeProtocolError
		}

		if decodeFunc, ok := propertyDecodeType[propertyTypeMap[id]]; ok {
			var decodeCount int
			decodeCount, err = decodeFunc(p, id, slice[count:])
			count += decodeCount
			offset += count
			if err != nil {
				return offset, err
			}
		} else {
			return offset, CodeProtocolError
		}

		p.len += uint32(count)
		pLen -= uint32(count)
	}

	return offset, nil
}

func (p *property) encode(to []byte) (int, error) {
	pLen := p.FullLen()
	if int(pLen) > len(to) {
		return 0, ErrInsufficientBufferSize
	}

	if pLen == 1 {
		return 1, nil
	}

	var offset int
	var err error
	// Encode variable length header
	total := binary.PutUvarint(to, uint64(p.len))

	for k, v := range p.properties {
		fn := propertyEncodeType[propertyTypeMap[k]]
		offset, err = fn(k, v, to[total:])
		total += offset

		if err != nil {
			break
		}
	}

	return total, err
}

func calcLenByte(id PropertyID, val interface{}) (int, error) {
	l := 0
	calc := func() int {
		return 1 + uvarintCalc(uint32(id))
	}

	switch valueType := val.(type) {
	case uint8:
		l = calc()
	case []uint8:
		for range valueType {
			l += calc()
		}
	default:
		return 0, nil
	}

	return l, nil
}

func calcLenShort(id PropertyID, val interface{}) (int, error) {
	l := 0

	calc := func() int {
		return 2 + uvarintCalc(uint32(id))
	}

	switch valueType := val.(type) {
	case uint16:
		l = calc()
	case []uint16:
		for range valueType {
			l += calc()
		}
	default:
		return 0, nil
	}

	return l, nil
}

func calcLenInt(id PropertyID, val interface{}) (int, error) {
	l := 0

	calc := func() int {
		return 4 + uvarintCalc(uint32(id))
	}

	switch valueType := val.(type) {
	case uint32:
		l = calc()
	case []uint32:
		for range valueType {
			l += calc()
		}
	default:
		return 0, nil
	}

	return l, nil
}

func calcLenVarInt(id PropertyID, val interface{}) (int, error) {
	l := 0

	calc := func(v uint32) int {
		return uvarintCalc(v) + uvarintCalc(uint32(id))
	}

	switch valueType := val.(type) {
	case uint32:
		l = calc(valueType)
	case []uint32:
		for _, v := range valueType {
			l += calc(v)
		}
	default:
		return 0, nil
	}

	return l, nil
}

func calcLenString(id PropertyID, val interface{}) (int, error) {
	l := 0

	calc := func(n int) int {
		return 2 + n + uvarintCalc(uint32(id))
	}

	switch valueType := val.(type) {
	case string:
		l = calc(len(valueType))
	case []string:
		for _, v := range valueType {
			l += calc(len(v))
		}
	default:
		return 0, nil
	}

	return l, nil
}

func calcLenBinary(id PropertyID, val interface{}) (int, error) {
	l := 0

	calc := func(n int) int {
		return 2 + n + uvarintCalc(uint32(id))
	}

	switch valueType := val.(type) {
	case []byte:
		l = calc(len(valueType))
	case [][]string:
		for _, v := range valueType {
			l += calc(len(v))
		}
	default:
		return 0, nil
	}

	return l, nil
}

func calcLenStringPair(id PropertyID, val interface{}) (int, error) {
	l := 0

	calc := func(k, v int) int {
		return 4 + k + v + uvarintCalc(uint32(id))
	}

	switch valueType := val.(type) {
	case StringPair:
		l = calc(len(valueType.K), len(valueType.V))
	case []StringPair:
		for _, v := range valueType {
			l += calc(len(v.K), len(v.V))
		}
	default:
		return 0, nil
	}

	return l, nil
}

func decodeByte(p *property, id PropertyID, from []byte) (int, error) {
	offset := 0
	if len(from[offset:]) < 1 {
		return offset, CodeMalformedPacket
	}

	p.properties[id] = from[offset]
	offset++

	return offset, nil
}

func decodeShort(p *property, id PropertyID, from []byte) (int, error) {
	offset := 0
	if len(from[offset:]) < 2 {
		return offset, CodeMalformedPacket
	}

	v := binary.BigEndian.Uint16(from[offset:])
	offset += 2

	p.properties[id] = v

	return offset, nil
}

func decodeInt(p *property, id PropertyID, from []byte) (int, error) {
	offset := 0
	if len(from[offset:]) < 4 {
		return offset, CodeMalformedPacket
	}

	v := binary.BigEndian.Uint32(from[offset:])
	offset += 4

	p.properties[id] = v

	return offset, nil
}

func decodeVarInt(p *property, id PropertyID, from []byte) (int, error) {
	offset := 0

	v, cnt := uvarint(from[offset:])
	if cnt <= 0 {
		return offset, CodeMalformedPacket
	}
	offset += cnt

	p.properties[id] = v

	return offset, nil
}

func decodeString(p *property, id PropertyID, from []byte) (int, error) {
	offset := 0

	v, n, err := ReadLPBytes(from[offset:])
	if err != nil || !utf8.Valid(v) {
		return offset, CodeMalformedPacket
	}

	offset += n

	p.properties[id] = string(v)

	return offset, nil
}

func decodeStringPair(p *property, id PropertyID, from []byte) (int, error) {
	var k []byte
	var v []byte
	var n int
	var err error

	k, n, err = ReadLPBytes(from)
	offset := n
	if err != nil || !utf8.Valid(k) {
		return offset, CodeMalformedPacket
	}

	v, n, err = ReadLPBytes(from[offset:])
	offset += n

	if err != nil || !utf8.Valid(v) {
		return offset, CodeMalformedPacket
	}

	if _, ok := p.properties[id]; !ok {
		p.properties[id] = []StringPair{}
	}

	p.properties[id] = append(p.properties[id].([]StringPair), StringPair{K: string(k), V: string(v)})

	return offset, nil
}

func decodeBinary(p *property, id PropertyID, from []byte) (int, error) {
	offset := 0

	b, n, err := ReadLPBytes(from[offset:])
	if err != nil {
		return offset, CodeMalformedPacket
	}
	offset += n

	tmp := make([]byte, len(b))

	copy(tmp, b)

	p.properties[id] = tmp

	return offset, nil
}

func encodeByte(id PropertyID, val interface{}, to []byte) (int, error) {
	offset := 0

	encode := func(v uint8, to []byte) int {
		off := writePrefixID(id, to)

		to[off] = v
		off++

		return off
	}

	switch valueType := val.(type) {
	case uint8:
		offset += encode(valueType, to[offset:])
	case []uint8:
		for _, v := range valueType {
			offset += encode(v, to[offset:])
		}
	}

	return offset, nil
}

func encodeShort(id PropertyID, val interface{}, to []byte) (int, error) {
	offset := 0

	encode := func(v uint16, to []byte) int {
		off := writePrefixID(id, to)
		binary.BigEndian.PutUint16(to[off:], v)
		off += 2

		return off
	}

	switch valueType := val.(type) {
	case uint16:
		offset += encode(valueType, to[offset:])
	case []uint16:
		for _, v := range valueType {
			offset += encode(v, to[offset:])
		}
	}

	return offset, nil
}

func encodeInt(id PropertyID, val interface{}, to []byte) (int, error) {
	offset := 0

	encode := func(v uint32, to []byte) int {
		off := writePrefixID(id, to)
		binary.BigEndian.PutUint32(to[off:], v)
		off += 4

		return off
	}

	switch valueType := val.(type) {
	case uint32:
		offset += encode(valueType, to[offset:])
	case []uint32:
		for _, v := range valueType {
			offset += encode(v, to[offset:])
		}
	}

	return offset, nil
}

func encodeVarInt(id PropertyID, val interface{}, to []byte) (int, error) {
	offset := 0

	encode := func(v uint32, to []byte) int {
		off := writePrefixID(id, to)
		off += binary.PutUvarint(to[off:], uint64(v))

		return off
	}

	switch valueType := val.(type) {
	case uint32:
		offset += encode(valueType, to[offset:])
	case []uint32:
		for _, v := range valueType {
			offset += encode(v, to[offset:])
		}
	}

	return offset, nil
}

func encodeString(id PropertyID, val interface{}, to []byte) (int, error) {
	offset := 0

	encode := func(v string, to []byte) int {
		off := writePrefixID(id, to)
		count, _ := WriteLPBytes(to[off:], []byte(v))
		off += count

		return off
	}

	switch valueType := val.(type) {
	case string:
		offset += encode(valueType, to[offset:])
	case []string:
		for _, v := range valueType {
			offset += encode(v, to[offset:])
		}
	}

	return offset, nil
}

func encodeStringPair(id PropertyID, val interface{}, to []byte) (int, error) {
	offset := 0

	encode := func(v StringPair, to []byte) int {
		off := writePrefixID(id, to)

		n, _ := WriteLPBytes(to[off:], []byte(v.K))
		off += n

		n, _ = WriteLPBytes(to[off:], []byte(v.V))
		off += n

		return off
	}

	switch valueType := val.(type) {
	case StringPair:
		offset += encode(valueType, to[offset:])
	case []StringPair:
		for _, v := range valueType {
			offset += encode(v, to[offset:])
		}
	}

	return offset, nil
}

func encodeBinary(id PropertyID, val interface{}, to []byte) (int, error) {
	offset := 0

	encode := func(v []byte, to []byte) int {
		off := writePrefixID(id, to)
		count, _ := WriteLPBytes(to[off:], v)
		off += count

		return off
	}

	switch valueType := val.(type) {
	case []byte:
		offset += encode(valueType, to[offset:])
	case [][]byte:
		for _, v := range valueType {
			offset += encode(v, to[offset:])
		}
	}
	return offset, nil
}
