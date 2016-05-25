package codec

import (
	b "encoding/binary"
	"io"
)

// Read reads structured binary data from r into data.
// Data must be a pointer to a fixed-size value or a slice
// of fixed-size values.
// Bytes read from r are decoded using the specified byte order
// and written to successive fields of the data.
// When reading into structs, the field data for fields with
// blank (_) field names is skipped; i.e., blank field names
// may be used for padding.
// When reading into a struct, all non-blank fields must be exported.
func Read(r io.Reader, order b.ByteOrder, data interface{}) error {
	arr, ok := data.([]uint8)
	if ok {
		if _, err := io.ReadFull(r, arr); err != nil {
			return err
		}
		return nil
	} else {
		return b.Read(r, order, data)
	}
}

// Write writes the binary representation of data into w.
// Data must be a fixed-size value or a slice of fixed-size
// values, or a pointer to such data.
// Bytes written to w are encoded using the specified byte order
// and read from successive fields of the data.
// When writing structs, zero values are written for fields
// with blank (_) field names.
func Write(w io.Writer, order b.ByteOrder, data interface{}) error {
	arr, ok := data.([]uint8)
	if ok {
		_, err := w.Write(arr)
		return err
	} else {
		return b.Write(w, order, data)
	}
}
