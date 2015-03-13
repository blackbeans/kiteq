package store

import (
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/proto"
	"github.com/sutoo/gorp"
	"kiteq/protocol"
	"log"
)

type Convertor struct{}

func (self Convertor) ToDb(val interface{}, fieldName string) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	//first bind by field name
	if fieldName == "Body" {
		s, stringOk := val.(string)
		if stringOk {
			return []byte(s), nil
		} else {
			b, byteOk := val.([]byte)
			if byteOk {
				return b, nil
			} else {
				return nil, errors.New("ToDb: unsuppoted type")
			}
		}
	}
	switch t := val.(type) {
	case []string:
		b, err := json.Marshal(t)
		if err != nil {
			return "", err
		}
		return string(b), nil
	case *protocol.Header:
		data, err := proto.Marshal(t)
		if err != nil {
			fmt.Printf("ToDB proto.Marshal failed.%T, %v \n", val, val)
			return "", err
		}
		return data, nil
	}
	return val, nil
}

var cs = gorp.CustomScanner{}

func (self Convertor) FromDb(target interface{}, fieldName string) (gorp.CustomScanner, bool) {
	if fieldName == "Body" {
		// var bodyArr []byte
		return cs, false
	}

	switch target.(type) {
	case *[]string:
		binder := func(holder, t interface{}) error {
			s, ok := holder.(*string)
			if !ok {
				return errors.New("FromDb: Unable to convert to *string")
			}
			b := []byte(*s)
			return json.Unmarshal(b, t)
		}
		return gorp.CustomScanner{new(string), target, binder}, true
	case **protocol.Header:
		binder := func(holder, t interface{}) error {

			b, ok := holder.(*[]byte)
			if !ok {
				return errors.New("FromDb: Unable to convert to string")
			}
			header := new(protocol.Header)
			if err := proto.Unmarshal(*b, header); err != nil {
				log.Printf("Convertor|Header|%s|%s\n", err, holder)
				return err
			}

			targetP, ok := t.(**protocol.Header)
			*targetP = header
			return nil
		}
		return gorp.CustomScanner{new([]byte), target, binder}, true
	default:
		// log.Printf("Convertor|FromDb|%s|%s\n", target, fieldName)
	}
	return cs, false
}
