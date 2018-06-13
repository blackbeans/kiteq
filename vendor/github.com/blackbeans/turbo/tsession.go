package turbo

import (
	"bufio"
	"bytes"
	"net"
	"time"

	log "github.com/blackbeans/log4go"
)



//turbo session
type TSession struct {
	conn       *net.TCPConn //tcp的session
	remoteAddr string
	br         *bufio.Reader
	bw         *bufio.Writer
	isClose    bool
	lasttime   uint32
	config     *TConfig
	onMessage  IOHandler
}

func NewSession(conn *net.TCPConn, config *TConfig,
	onMsg IOHandler) *TSession {

	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(config.IdleTime * 2)
	//禁用nagle
	conn.SetNoDelay(true)
	conn.SetReadBuffer(config.ReadBufferSize)
	conn.SetWriteBuffer(config.WriteBufferSize)

	session := &TSession{
		conn:         conn,
		br:           bufio.NewReaderSize(conn, config.ReadBufferSize),
		bw:           bufio.NewWriterSize(conn, config.WriteBufferSize),
		isClose:      false,
		remoteAddr:   conn.RemoteAddr().String(),
		onMessage: onMsg,
		config:    config}
	//连接数计数
	config.FlowStat.Connections.Incr(1)
	return session
}

func (self *TSession) RemotingAddr() string {
	return self.remoteAddr
}

func (self *TSession) Idle() bool {
	//当前时间如果大于 最后一次发包时间+2倍的idletime 则认为空心啊
	if uint32(time.Now().Unix()) -
			(self.lasttime+uint32(self.config.IdleTime / time.Second))>0{
		return true
	}
	return false
}

//读取
func (self *TSession) Open() {


	go func() {
		//缓存本次包的数据
		for !self.isClose {

			err := func() error {
				defer func() {
					if err := recover(); nil != err {
						log.ErrorLog("stderr", "TSession|Read|%s|recover|FAIL|%s", self.remoteAddr, err)
					}
				}()

				//按照标准的turbo packet读取packet头部
				buff, err := self.read0(self.br, PACKET_HEAD_LEN)
				if nil != err {
					self.Close()
					return err
				}

				br := bytes.NewReader(buff)
				head, err := UnmarshalHeader(br)
				if nil != err {
					log.ErrorLog("stderr", "TSession|UnmarshalHeader|%s|FAIL|CLOSE SESSION|%v",
						self.remoteAddr, err)
					self.onMessage(Packet{Header: head, Data: nil}, err)
					return err
				}

				if head.BodyLen > MAX_PACKET_BYTES {
					log.ErrorLog("stderr", "TSession|UnmarshalHeader|%s|Too Large Packet|CLOSE SESSION|%v",
						self.remoteAddr, head.BodyLen)
					//构造一个error的响应包
					self.onMessage(Packet{Header: head, Data: nil}, ERR_TOO_LARGE_PACKET)
					return ERR_TOO_LARGE_PACKET
				}

				//读取body
				body, err := self.read0(self.br, int(head.BodyLen))
				if nil != err {
					log.ErrorLog("stderr", "TSession|ReadBody|%s|FAIL|CLOSE SESSION|%v|bodyLen:%d",
						self.remoteAddr, err, head.BodyLen)
					//构造一个error的读取包
					self.onMessage(Packet{Header: head, Data: nil}, err)
					return err
				}

				p := Packet{Header: head, Data: body}
				//回调上层
				self.onMessage(p, nil)
				if nil != self.config.FlowStat {
					self.config.FlowStat.ReadFlow.Incr(1)
					self.config.FlowStat.ReadBytesFlow.Incr(PACKET_HEAD_LEN + head.BodyLen)
				}
				return nil
			}()
			if nil != err {
				break
			}
		}
	}()
}

//分段读取
func (self *TSession) read0(br *bufio.Reader, len int) ([]byte, error) {
	//按照标准的turbo packet读取packet头部
	buff := make([]byte, len)
	idx := 0
	for {
		l, err := br.Read(buff[idx:])
		if nil != err {
			log.ErrorLog("stderr","TSession|Open|%s|FAIL|CLOSE SESSION|%s", self.remoteAddr, err)
			return nil, err
		}
		idx += l
		if idx >= len {
			break
		}
	}
	return buff, nil

}


//写数据
func(self *TSession) Write(tlv ...*Packet) error{

	self.lasttime = uint32(time.Now().Unix())
	batch := make([]byte, 0, len(tlv)*128)
	for _, t := range tlv {
		p := t.Marshal()
		if nil == p || len(p) <= 0 {
			log.ErrorLog("stderr","TSession|asyncWrite|MarshalPayload|FAIL|EMPTY PACKET|%s|%v", t)
			if nil != t.OnComplete {
				t.OnComplete(ERR_MARSHAL_PACKET)
			}
			//如果是同步写出
			continue
		}

		//如果大小超过了最大值那么久写入失败
		if t.Header.BodyLen > MAX_PACKET_BYTES {
			if nil != t.OnComplete {
				log.ErrorLog("stderr","TSession|asyncWrite|MarshalPayload|FAIL|MAX_PACKET_BYTES|%s|%d/%d",
					t.Header.BodyLen,MAX_PACKET_BYTES)
				t.OnComplete(ERR_TOO_LARGE_PACKET)
			}
			continue
		}else{
			//成功写出去了
			t.OnComplete(nil)
		}
		batch = append(batch, p...)
	}

	if len(batch) <= 0 {
		return nil
	}

	l := 0
	tmp := batch
	for {
		length, err := self.bw.Write(tmp)
		if nil != err {
			log.ErrorLog("stderr","TSession|asyncWrite|conn|%s|FAIL|%s|%d/%d", self.remoteAddr, err, length, len(tmp))
			return err
		}

		l += length
		//write finish
		if l == len(batch) {
			break
		}
		tmp = batch[l:]
	}
	// //flush
	self.bw.Flush()
	if nil != self.config.FlowStat {
		self.config.FlowStat.WriteFlow.Incr(int32(len(tlv)))
		self.config.FlowStat.WriteBytesFlow.Incr(int32(len(batch)))
	}
	return nil
}

//当前连接是否关闭
func (self *TSession) Closed() bool {
	return self.isClose
}

func (self *TSession) Close() error {

	if !self.isClose {
		self.isClose = true
		//flush
		self.bw.Flush()
		self.conn.Close()
		self.config.FlowStat.Connections.Incr(-1)
		log.InfoLog("stdout","TSession|Close|%s...", self.remoteAddr)
	}

	return nil
}
