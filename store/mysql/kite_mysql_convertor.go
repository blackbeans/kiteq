package mysql

import (
	"encoding/json"

	"github.com/blackbeans/kiteq-common/protocol"
	"kiteq/store"
	"reflect"
)

type convertor struct {
	columns []column
}

func (self convertor) convertFields(entity *store.MessageEntity, filter func(colname string) bool) []interface{} {
	dest := make([]interface{}, 0, len(self.columns))
	rv := reflect.ValueOf(entity)
	elem := rv.Elem()
	for _, c := range self.columns {
		if filter(c.columnName) {
			continue
		}
		fb := elem.FieldByName(c.fieldName).Addr().Interface()
		if c.fieldKind == reflect.Slice || c.fieldKind == reflect.Array {
			var a string
			dest = append(dest, &a)
		} else if c.columnName == "header" || c.columnName == "body" {
			var h []byte
			dest = append(dest, &h)
		} else {
			dest = append(dest, fb)
		}
	}
	return dest

}

func (self convertor) Convert2Entity(fv []interface{}, entity *store.MessageEntity, filter func(colname string) bool) {
	val := reflect.ValueOf(entity)
	elem := val.Elem()
	//可能存在过滤的值需要记录应该读取那个位置的参数
	j := 0
	for i, c := range self.columns {
		if filter(c.columnName) {
			j++
			continue
		}
		i = i - j
		v := fv[i]
		rval := reflect.ValueOf(v)
		rv := rval.Elem().Interface()
		fn := elem.FieldByName(c.fieldName)

		k := fn.Kind()

		// log.Debug("convertor|Convert2Entity|%s|%s", c.fieldName, rv)

		switch k {
		case reflect.Int8:
			fn.Set(reflect.ValueOf(rv.(int8)))
		case reflect.Int:
			fn.Set(reflect.ValueOf(rv.(int)))
		case reflect.Int16:
			fn.Set(reflect.ValueOf(rv.(int16)))
		case reflect.Int32:
			fn.Set(reflect.ValueOf(rv.(int32)))
		case reflect.Int64:
			fn.SetInt((rv.(int64)))
		case reflect.Uint8:
			fn.Set(reflect.ValueOf(rv.(uint8)))
		case reflect.Uint:
			fn.Set(reflect.ValueOf(rv.(uint)))
		case reflect.Uint16:
			fn.Set(reflect.ValueOf(rv.(uint16)))
		case reflect.Uint32:
			fn.Set(reflect.ValueOf(rv.(uint32)))
		case reflect.Uint64:
			fn.SetUint((rv.(uint64)))
		case reflect.Bool:
			fn.SetBool(rv.(bool))
		case reflect.String:
			fn.SetString((rv.(string)))
		case reflect.Ptr:
			{
				hd, hok := rv.([]byte)
				_, ok := fn.Interface().(*protocol.Header)
				if ok && hok {
					var header protocol.Header
					if hok {
						//头部用PB反序列化
						err := protocol.UnmarshalPbMessage(hd, &header)
						if nil != err {
							log.Errorf("convertor|Convert2Entity|Unmarshal Header|FAIL|%s|%s", err, c.fieldName)
						}
					}
					fn.Set(reflect.ValueOf(&header))
				} else if hok {
					fn.SetBytes(hd)
				} else {
					log.Errorf("convertor|Convert2Entity|FAIL|UnSupport Ptr DataType|%s|%t|%s|%s", c.fieldName, rv, hok, ok)
					return
				}
			}
		case reflect.Array, reflect.Slice:
			k := fn.Type().Elem().Kind()
			if k == reflect.String {
				var data []string
				err := json.Unmarshal([]byte(rv.(string)), &data)
				if nil != err {
					log.Errorf("convertor|Convert2Entity|FAIL|UnSupport SLICE|%s|%s", c.fieldName, rv)
				}
				fn.Set(reflect.ValueOf(data))
			} else if k == reflect.Uint8 {
				fn.SetBytes(rv.([]byte))
			} else {
				log.Errorf("convertor|Convert2Entity|FAIL|UnSupport SLICE DataType|%s|%s", c.columnName, fn.Elem().Kind())
				return
			}
		default:
			if c.columnName == "body" {
				_, ok := rv.([]byte)
				if ok {
					fn.Set(rval.Elem())
				} else {
					log.Errorf("convertor|Convert2Entity|FAIL|UnSupport BodyType |REQ:[]byte|%s|%T", c.fieldName, rv)
					return
				}
			} else {
				log.Errorf("convertor|Convert2Entity|FAIL|UnSupport DataType|%s|%s", c.fieldName, rv)
			}
		}
	}
}

func (self convertor) Convert2Params(entity *store.MessageEntity) []interface{} {

	val := reflect.ValueOf(*entity)
	fvs := make([]interface{}, 0, len(self.columns))
	for _, v := range self.columns {

		var fv interface{}
		if v.columnName == "body" {
			if entity.MsgType == protocol.CMD_STRING_MESSAGE {
				fv = []byte(entity.GetBody().(string))
			} else if entity.MsgType == protocol.CMD_BYTES_MESSAGE {
				fv = entity.GetBody().([]byte)
			} else {
				log.Errorf("convertor|Convert2Params|UnSupport MESSAGE TYPE|%s", entity.MsgType)
			}
		} else {
			f := val.FieldByName(v.fieldName)

			// log.Debug("convertor|Convert2Params|%s|%s", v.fieldName, f)

			switch f.Kind() {
			case reflect.Ptr:
				header, ok := f.Interface().(*protocol.Header)
				if ok {
					//头部用Pb序列化
					data, err := protocol.MarshalPbMessage(header)
					if err != nil {
						log.Errorf("convertor|Convert2Params|Marshal|HEAD|FAIL|%s|%s", err, f.Addr().Interface())
						return nil
					}
					fv = data
				} else {
					log.Errorf("convertor|Convert2Params|Not protocol.Header PRT |FAIL|%s", f.Addr())
					return nil
				}

			case reflect.Slice, reflect.Array:

				if f.Type().Elem().Kind() == reflect.String {
					data, err := json.Marshal(f.Interface())
					if nil != err {
						log.Errorf("convertor|Convert2Params|Marshal|Slice|FAIL||%s", err)
						return nil
					}
					fv = string(data)
				} else {
					fv = f.Interface()
				}

			default:
				fv = f.Interface()
			}
		}
		fvs = append(fvs, &fv)
	}

	return fvs

}
