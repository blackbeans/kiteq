package mysql

import (
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"kiteq/protocol"
	"kiteq/store"
	"log"
	"reflect"
)

type convertor struct {
	columns []column
}

func (self convertor) Convert2Entity(fv []interface{}) *store.MessageEntity {
	entity := &store.MessageEntity{}
	val := reflect.ValueOf(entity)
	for i, v := range fv {
		c := self.columns[i]
		fn := val.FieldByName(c.fieldName)
		k := fn.Kind()
		if (k == reflect.Int) || (k == reflect.Int16) || (k == reflect.Int32) || (k == reflect.Int64) {
			fn.SetInt(v.(int64))
		} else if (k == reflect.Uint) || (k == reflect.Uint16) || (k == reflect.Uint32) || (k == reflect.Uint64) {
			fn.SetUint(v.(uint64))
		} else if k == reflect.Bool {
			fn.SetBool(v.(bool))
		} else if k == reflect.String {
			fn.SetString(v.(string))
		} else if k == reflect.Slice {
			hd, ok := v.([]byte)
			if ok && c.columnName == "header" {
				err := proto.Unmarshal(hd, entity.Header)
				if nil != err {
					log.Printf("convertor|Convert2Entity|Unmarshal Header|FAIL|%s|%s\n", err, c.fieldName)
				}
			} else if ok {
				fn.SetBytes(hd)
			}

		} else {
			log.Printf("convertor|Convert2Entity|FAIL|UnSupport DataType|%s\n", c.fieldName)
			return nil
		}
	}
	return entity
}

func (self convertor) Convert2Params(entity *store.MessageEntity) []interface{} {

	val := reflect.ValueOf(entity)
	fvs := make([]interface{}, 0, len(self.columns))
	for _, v := range self.columns {
		f := val.FieldByName(v.fieldName)
		var fv interface{}
		if v.columnName == "body" && entity.MsgType == protocol.CMD_STRING_MESSAGE {
			fv = []byte(entity.GetBody().(string))
		}

		switch f.Kind() {
		case reflect.Ptr:
			header, ok := f.Addr().Interface().(*protocol.Header)
			if ok {
				data, err := proto.Marshal(header)
				if err != nil {
					log.Printf("convertor|Convert2Params|Marshal|HEAD|FAIL|%s|%s\n", err, f.Addr().Interface())
					return nil
				}
				fv = data
			} else {
				log.Printf("convertor|Convert2Params|Not protocol.Header Point |FAIL|%s\n", f.Addr())
				return nil
			}

		case reflect.Slice:
			data, err := json.Marshal(f.Addr().Interface())
			if nil != err {
				log.Printf("convertor|Convert2Params|Marshal|Slice|FAIL||%s\n", err)
				return nil
			}
			fv = data
		default:
			fv = f.Interface()
		}
		fvs = append(fvs, fv)
	}

	return fvs

}
