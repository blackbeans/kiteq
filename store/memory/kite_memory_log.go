package memory

import (
	"encoding/json"
	log "github.com/blackbeans/log4go"
)

const (
	OP_C = byte(0) //create
	OP_U = byte(1) //update
	OP_D = byte(2) //delete
	OP_E = byte(4) //expired
)

//data operation log
type oplog struct {
	Op      byte      `json:"op"`
	LogicId string    `json:"logic_id"`
	ChunkId int64     `json:"chunk_id"`
	Status  ChunkFlag `json:"status"`
	Body    string    `json:"body"`
}

func newOplog(op byte, logicId string, chunkid int64, status ChunkFlag, body string) *oplog {
	return &oplog{
		Op:      op,
		LogicId: logicId,
		ChunkId: chunkid,
		Status:  status,
		Body:    body}
}

//marshal oplog
func (self *oplog) marshal() []byte {
	d, err := json.Marshal(self)
	if nil != err {
		log.Error("oplog|marshal|fail|%s|%s", err, self)
		return nil
	}
	return d
}

//unmarshal data
func (self *oplog) unmarshal(data []byte) error {
	return json.Unmarshal(data, self)

}
