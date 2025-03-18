package roaring

import (
	"unsafe"
)

func (c *Container) Encode() []byte {
	if c == nil {
		return nil
	}
	switch c.typeID {
	case ContainerArray:
		a := c.array()
		if len(a) > ArrayMaxSize {
			c.arrayToBitmap()
			return append(fromArray64(c.bitmap()), byte(ContainerBitmap))
		}
		return append(fromArray16(a), c.typeID)
	case ContainerRun:
		r := c.runs()
		if len(r) > runMaxSize {
			c.bitmap()
			return append(fromArray64(c.bitmap()), byte(ContainerBitmap))
		}
		return append(fromInterval16(r), c.typeID)
	case ContainerBitmap:
		return append(fromArray64(c.bitmap()), c.typeID)
	default:
		return nil
	}
}

func (c *Container) EncodeTo(buf []byte) []byte {
	if c == nil {
		return nil
	}
	buf = buf[:0]
	switch c.typeID {
	case ContainerArray:
		a := c.array()
		if len(a) > ArrayMaxSize {
			c.arrayToBitmap()
			buf = append(buf, fromArray64(c.bitmap())...)
			return append(buf, byte(ContainerBitmap))
		}
		buf = append(buf, fromArray16(a)...)
		return append(buf, c.typeID)
	case ContainerRun:
		r := c.runs()
		if len(r) > runMaxSize {
			c.bitmap()
			buf = append(buf, fromArray64(c.bitmap())...)
			return append(buf, byte(ContainerBitmap))
		}
		buf = append(buf, fromInterval16(r)...)
		return append(buf, c.typeID)
	case ContainerBitmap:
		buf = append(buf, fromArray64(c.bitmap())...)
		return append(buf, c.typeID)
	default:
		return nil
	}
}

func DecodeContainer(value []byte) *Container {
	data, typ := separate(value)
	switch typ {
	case ContainerArray:
		d := toArray16(data)
		return NewContainerArray(d)
	case ContainerBitmap:
		d := toArray64(data)
		return NewContainerBitmap(-1, d)
	case ContainerRun:
		d := toInterval16(data)
		return NewContainerRun(d)
	default:
		return nil
	}
}

func LastValueFromEncodedContainer(value []byte) uint16 {
	data, typ := separate(value)
	switch typ {
	case ContainerArray:
		a := toArray16(data)
		return a[len(a)-1]
	case ContainerRun:
		r := toInterval16(data)
		return r[len(r)-1].Last
	case ContainerBitmap:
		a := toArray64(data)
		return lastValueFromBitmap(a)
	default:
		return 0
	}
}

func lastValueFromBitmap(a []uint64) uint16 {
	for i := len(a) - 1; i >= 0; i-- {
		for j := 63; j >= 0; j-- {
			if a[i]&(1<<j) != 0 {
				return (uint16(i) * 64) + uint16(j)
			}
		}
	}
	return 0
}

func separate(data []byte) (co []byte, typ byte) {
	return data[:len(data)-1], data[len(data)-1]
}

// toArray16 converts a byte slice into a slice of uint16 values using unsafe.
func toArray16(a []byte) []uint16 {
	return (*[4096]uint16)(unsafe.Pointer(&a[0]))[: len(a)/2 : len(a)/2]
}

// toArray64 converts a byte slice into a slice of uint64 values using unsafe.
func toArray64(a []byte) []uint64 {
	return (*[1024]uint64)(unsafe.Pointer(&a[0]))[:1024:1024]
}

// toArray16 converts a byte slice into a slice of uint16 values using unsafe.
func toInterval16(a []byte) []Interval16 {
	return (*[2048]Interval16)(unsafe.Pointer(&a[0]))[: len(a)/4 : len(a)/4]
}
