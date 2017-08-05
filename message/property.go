package message

import "encoding/binary"

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
)

// Error
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
	default:
		return "property: unknown error"
	}
}

type KVPair struct {
	Key   string
	Value string
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

var propertyAllowedMessageTypes = map[PropertyID]map[PacketType]bool{
	PropertyPayloadFormat:            {PUBLISH: false},
	PropertyPublicationExpiry:        {PUBLISH: false},
	PropertyContentType:              {PUBLISH: false},
	PropertyResponseTopic:            {PUBLISH: false},
	PropertyCorrelationData:          {PUBLISH: false},
	PropertySubscriptionIdentifier:   {PUBLISH: true, SUBSCRIBE: false},
	PropertySessionExpiryInterval:    {CONNECT: false, DISCONNECT: false},
	PropertyAssignedClientIdentifier: {CONNACK: false},
	PropertyServerKeepAlive:          {CONNACK: false},
	PropertyAuthMethod:               {CONNECT: false, CONNACK: false, AUTH: false},
	PropertyAuthData:                 {CONNECT: false, CONNACK: false, AUTH: false},
	PropertyRequestProblemInfo:       {CONNECT: false},
	PropertyWillDelayInterval:        {CONNECT: false},
	PropertyRequestResponseInfo:      {CONNECT: false},
	PropertyResponseInfo:             {CONNACK: false},
	PropertyServerReverence:          {CONNACK: false, DISCONNECT: false},
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
	PropertyReceiveMaximum:    {CONNECT: false, CONNACK: false},
	PropertyTopicAliasMaximum: {CONNECT: false, CONNACK: false},
	PropertyTopicAlias:        {PUBLISH: false},
	PropertyMaximumQoS:        {CONNACK: false},
	PropertyRetainAvailable:   {CONNACK: false},
	PropertyUserProperty: {
		CONNECT:    false,
		CONNACK:    false,
		PUBLISH:    false,
		PUBACK:     false,
		PUBREC:     false,
		PUBREL:     false,
		PUBCOMP:    false,
		SUBACK:     false,
		UNSUBACK:   false,
		DISCONNECT: false,
		AUTH:       false},
	PropertyMaximumPacketSize:               {CONNECT: false, CONNACK: false},
	PropertyWildcardSubscriptionAvailable:   {CONNACK: false},
	PropertySubscriptionIdentifierAvailable: {CONNACK: false},
	PropertySharedSubscriptionAvailable:     {CONNACK: false},
}

//var propertyTypeMap = map[PropertyID]struct {
//	p PropertyType
//	n interface{}
//}{
//	PropertyPayloadFormat:                   {p: PropertyTypeByte, n: uint8(0)},
//	PropertyPublicationExpiry:               {p: PropertyTypeInt, n: uint32(0)},
//	PropertyContentType:                     {p: PropertyTypeString, n: ""},
//	PropertyResponseTopic:                   {p: PropertyTypeString, n: ""},
//	PropertyCorrelationData:                 {p: PropertyTypeBinary, n: []byte{}},
//	PropertySubscriptionIdentifier:          {p: PropertyTypeVarInt, n: uint32(0)},
//	PropertySessionExpiryInterval:           {p: PropertyTypeInt, n: uint32(0)},
//	PropertyAssignedClientIdentifier:        {p: PropertyTypeString, n: ""},
//	PropertyServerKeepAlive:                 {p: PropertyTypeShort, n: uint16(0)},
//	PropertyAuthMethod:                      {p: PropertyTypeString, n: ""},
//	PropertyAuthData:                        {p: PropertyTypeBinary, n: []byte{}},
//	PropertyRequestProblemInfo:              {p: PropertyTypeByte, n: uint8(0)},
//	PropertyWillDelayInterval:               {p: PropertyTypeInt, n: uint32(0)},
//	PropertyRequestResponseInfo:             {p: PropertyTypeByte, n: uint8(0)},
//	PropertyResponseInfo:                    {p: PropertyTypeString, n: ""},
//	PropertyServerReverence:                 {p: PropertyTypeString, n: ""},
//	PropertyReasonString:                    {p: PropertyTypeString, n: ""},
//	PropertyReceiveMaximum:                  {p: PropertyTypeShort, n: uint16(0)},
//	PropertyTopicAliasMaximum:               {p: PropertyTypeShort, n: uint16(0)},
//	PropertyTopicAlias:                      {p: PropertyTypeShort, n: uint16(0)},
//	PropertyMaximumQoS:                      {p: PropertyTypeByte, n: uint8(0)},
//	PropertyRetainAvailable:                 {p: PropertyTypeByte, n: uint8(0)},
//	PropertyUserProperty:                    {p: PropertyTypeString, n: ""},
//	PropertyMaximumPacketSize:               {p: PropertyTypeInt, n: uint32(0)},
//	PropertyWildcardSubscriptionAvailable:   {p: PropertyTypeByte, n: uint8(0)},
//	PropertySubscriptionIdentifierAvailable: {p: PropertyTypeByte, n: uint8(0)},
//	PropertySharedSubscriptionAvailable:     {p: PropertyTypeByte, n: uint8(0)},
//}

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
	PropertyUserProperty:                    PropertyTypeString,
	PropertyMaximumPacketSize:               PropertyTypeInt,
	PropertyWildcardSubscriptionAvailable:   PropertyTypeByte,
	PropertySubscriptionIdentifierAvailable: PropertyTypeByte,
	PropertySharedSubscriptionAvailable:     PropertyTypeByte,
}

var propertyTypeDup = map[PropertyID]bool{
	PropertyPayloadFormat:                   false,
	PropertyPublicationExpiry:               false,
	PropertyContentType:                     false,
	PropertyResponseTopic:                   false,
	PropertyCorrelationData:                 false,
	PropertySubscriptionIdentifier:          false,
	PropertySessionExpiryInterval:           false,
	PropertyAssignedClientIdentifier:        false,
	PropertyServerKeepAlive:                 false,
	PropertyAuthMethod:                      false,
	PropertyAuthData:                        false,
	PropertyRequestProblemInfo:              false,
	PropertyWillDelayInterval:               false,
	PropertyRequestResponseInfo:             false,
	PropertyResponseInfo:                    false,
	PropertyServerReverence:                 false,
	PropertyReasonString:                    false,
	PropertyReceiveMaximum:                  false,
	PropertyTopicAliasMaximum:               false,
	PropertyTopicAlias:                      false,
	PropertyMaximumQoS:                      false,
	PropertyRetainAvailable:                 false,
	PropertyUserProperty:                    true,
	PropertyMaximumPacketSize:               false,
	PropertyWildcardSubscriptionAvailable:   false,
	PropertySubscriptionIdentifierAvailable: false,
	PropertySharedSubscriptionAvailable:     false,
}

func newProperty() *property {
	p := &property{
		properties: make(map[PropertyID]interface{}),
	}

	return p
}

// DupAllowed check if property id allows keys duplication
func (p PropertyID) DupAllowed() bool {
	d, ok := propertyTypeDup[p]
	if !ok {
		return false
	}

	return d
}

// IsValid check if property id is valid spec value
func (p PropertyID) IsValid() bool {
	if _, ok := propertyTypeMap[p]; ok {
		return true
	}

	return false
}

// IsValidPacketType check either property id can be used for given packet type
func (p PropertyID) IsValidPacketType(t PacketType) bool {
	mT, ok := propertyAllowedMessageTypes[p]
	if !ok {
		return false
	}

	if _, ok = mT[t]; !ok {
		return false
	}

	return true
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
func (p *property) Set(t PacketType, id PropertyID, val interface{}) error {
	if mT, ok := propertyAllowedMessageTypes[id]; !ok {
		return ErrPropertyInvalidID
	} else if _, ok = mT[t]; !ok {
		return ErrPropertyPacketTypeMismatch
	}

	// Todo: check type allowed for id

	// calculate property size
	switch valueType := val.(type) {
	case uint8:
		p.len++
	case uint16:
		p.len += 2
	case uint32:
		p.len += 4
	case string:
		p.len += uint32(len(valueType))
	case []string:
		for i := range valueType {
			p.len += uint32(len(valueType[i]))
		}
	case []byte:
		p.len += uint32(len(valueType))
	case [][]byte:
		for i := range valueType {
			p.len += uint32(len(valueType[i]))
		}
	case []uint16:
		p.len += uint32(len(valueType))
	case []uint32:
		p.len += uint32(len(valueType))
	default:
		return ErrPropertyUnsupported
	}

	p.properties[id] = val

	return nil
}

// Get property value
func (p *property) Get(id PropertyID) (interface{}, error) {
	if p, ok := p.properties[id]; ok {
		return p, nil
	}

	return nil, ErrPropertyNotFound
}

// ForEach iterate over existing properties
func (p *property) ForEach(f func(PropertyID, interface{})) {
	for k, v := range p.properties {
		f(k, v)
	}
}

func decodeProperties(t PacketType, buf []byte) (*property, int, error) {
	p := newProperty()

	total, err := p.decode(t, buf)
	if err != nil {
		return nil, total, err
	}

	// If properties are empty return only size of decoded property header
	if len(p.properties) == 0 {
		return nil, total, nil
	}

	return p, total, nil
}

func encodeProperties(p *property, dst []byte) (int, error) {
	if p == nil {
		if len(dst) > 0 {
			dst[0] = 0

		}
		return 1, nil
	}

	return p.encode(dst)
}

func (p *property) decode(t PacketType, buf []byte) (int, error) {
	total := 0
	// property length is encoded as variable byte integer
	pLen, lCount := uvarint(buf)
	if lCount <= 0 {
		return 0, CodeMalformedPacket
	}

	total += lCount

	for pLen != 0 {
		pidVal, pidCount := uvarint(buf[total:])
		if pidCount <= 0 {
			return total, CodeMalformedPacket
		}

		total += pidCount

		id := PropertyID(pidVal)

		if !id.IsValidPacketType(t) {
			return total, CodeMalformedPacket
		}

		if _, ok := p.properties[id]; ok && !id.DupAllowed() {
			return total, CodeProtocolError
		}

		total++

		count := 0

		switch propertyTypeMap[id] {
		case PropertyTypeByte:
			if len(buf[total+count:]) < 1 {
				return total + count, CodeMalformedPacket
			}

			v := buf[total+count]
			count++
			if _, ok := p.properties[id]; ok {
				return total + count, CodeMalformedPacket
			}
			if _, ok := p.properties[id]; !ok {
				p.properties[id] = []byte{v}
			} else {
				p.properties[id] = append(p.properties[id].([]byte), v)
			}
		case PropertyTypeShort:
			if len(buf[total+count:]) < 2 {
				return total + count, CodeMalformedPacket
			}

			v := binary.BigEndian.Uint16(buf[total+count:])
			count += 2

			if _, ok := p.properties[id]; !ok {
				p.properties[id] = []uint16{v}
			} else {
				p.properties[id] = append(p.properties[id].([]uint16), v)
			}
		case PropertyTypeInt:
			if len(buf[total+count:]) < 4 {
				return total + count, CodeMalformedPacket
			}

			v := binary.BigEndian.Uint32(buf[total:])
			count += 4

			if _, ok := p.properties[id]; !ok {
				p.properties[id] = []uint32{v}
			} else {
				p.properties[id] = append(p.properties[id].([]uint32), v)
			}
		case PropertyTypeVarInt:
			v, cnt := uvarint(buf[total+count:])
			if cnt <= 0 {
				return total + count, CodeMalformedPacket
			}
			count += cnt

			if _, ok := p.properties[id]; !ok {
				p.properties[id] = []uint32{v}
			} else {
				p.properties[id] = append(p.properties[id].([]uint32), v)
			}
		case PropertyTypeString:
			v, n, err := ReadLPBytes(buf[total+count:])
			if err != nil {
				return total + count, CodeMalformedPacket
			}
			count += n

			if _, ok := p.properties[id]; !ok {
				p.properties[id] = []string{string(v)}
			} else {
				p.properties[id] = append(p.properties[id].([]string), string(v))
			}
		case PropertyTypeStringPair:
			k, n, err := ReadLPBytes(buf[total+count:])
			if err != nil {
				return total + count, CodeMalformedPacket
			}
			count += n

			v, n, err := ReadLPBytes(buf[total+count:])
			if err != nil {
				return total + count, CodeMalformedPacket
			}
			count += n

			pair := KVPair{
				Key:   string(k),
				Value: string(v),
			}

			if _, ok := p.properties[id]; !ok {
				p.properties[id] = []KVPair{pair}
			} else {
				p.properties[id] = append(p.properties[id].([]KVPair), pair)
			}
		case PropertyTypeBinary:
			b, n, err := ReadLPBytes(buf[total+count:])
			if err != nil {
				return total + count, CodeMalformedPacket
			}
			count += n

			tmp := make([]byte, len(b))

			copy(tmp, b)
			if _, ok := p.properties[id]; !ok {
				p.properties[id] = [][]byte{tmp}
			} else {
				p.properties[id] = append(p.properties[id].([][]byte), tmp)
			}
		}

		p.len += uint32(count)
		pLen -= uint32(count)
		total += count
	}

	return total, nil
}

func (p *property) encode(buf []byte) (int, error) {
	pLen, pSizeCount := p.Len()
	if int(pLen)+pSizeCount > len(buf) {
		return 0, ErrInsufficientBufferSize
	}

	total := 0

	// Encode variable length header
	total += binary.PutUvarint(buf, uint64(p.len))

	writePrefixID := func(id PropertyID, b []byte) int {
		offset := 0
		b[offset] = byte(id)
		offset++

		return offset
	}

	for k, v := range p.properties {
		switch propertyTypeMap[k] {
		case PropertyTypeByte:
			switch valueType := v.(type) {
			case uint8:
				total += writePrefixID(k, buf)
				buf[total] = valueType
				total++
			case []uint8:
				for i := range valueType {
					total += writePrefixID(k, buf)
					buf[total] = valueType[i]
					total++
				}
			}
		case PropertyTypeShort:
			switch valueType := v.(type) {
			case uint16:
				total += writePrefixID(k, buf)
				binary.BigEndian.PutUint16(buf[total:], valueType)
				total += 2
			case []uint16:
				for i := range valueType {
					total += writePrefixID(k, buf)
					binary.BigEndian.PutUint16(buf[total:], valueType[i])
					total += 2
				}
			}
		case PropertyTypeInt:
			switch valueType := v.(type) {
			case uint32:
				total += writePrefixID(k, buf)
				binary.BigEndian.PutUint32(buf[total:], valueType)
				total += 4
			case []uint32:
				for i := range valueType {
					total += writePrefixID(k, buf)
					binary.BigEndian.PutUint32(buf[total:], valueType[i])
					total += 4
				}
			}
		case PropertyTypeVarInt:
			switch valueType := v.(type) {
			case uint32:
				total += writePrefixID(k, buf)
				total += binary.PutUvarint(buf[total:], uint64(valueType))
			case []uint32:
				for i := range valueType {
					total += writePrefixID(k, buf)
					total += binary.PutUvarint(buf[total:], uint64(valueType[i]))
				}
			}
		case PropertyTypeString:
			switch valueType := v.(type) {
			case string:
				total += writePrefixID(k, buf)
				total += copy(buf[total:], []byte(valueType))
			case []string:
				for i := range valueType {
					total += writePrefixID(k, buf)
					total += copy(buf[total:], []byte(valueType[i]))
				}
			}
		case PropertyTypeStringPair:
			switch valueType := v.(type) {
			case KVPair:
				total += writePrefixID(k, buf)
				n, err := WriteLPBytes(buf[total:], []byte(valueType.Key))
				if err != nil {
					return total, err
				}
				total += n

				n, err = WriteLPBytes(buf[total:], []byte(valueType.Key))
				if err != nil {
					return total, err
				}
				total += n
			case []KVPair:
				for i := range valueType {
					total += writePrefixID(k, buf)
					n, err := WriteLPBytes(buf[total:], []byte(valueType[i].Key))
					if err != nil {
						return total, err
					}
					total += n

					n, err = WriteLPBytes(buf[total:], []byte(valueType[i].Key))
					if err != nil {
						return total, err
					}
					total += n

				}
			}
		case PropertyTypeBinary:
			switch valueType := v.(type) {
			case []byte:
				total += writePrefixID(k, buf)
				total += copy(buf[total:], valueType)
			case [][]byte:
				for i := range valueType {
					total += writePrefixID(k, buf)
					total += copy(buf[total:], valueType[i])
				}
			}
		}
	}

	return total, nil
}
