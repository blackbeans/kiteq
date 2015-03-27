kiteq
=======

基于go+protobuff实现的多种持久化方案的mq框架

#### Client For KiteQ
    Go:    https://github.com/blackbeans/kiteq/tree/master/client
    Java : https://github.com/blackbeans/kiteq-client-java
    PHP:   https://github.com/blackbeans/kiteq-client-php

#### 简介
    * 基于zk维护发送方、订阅方、broker订阅发送关系、支持水平、垂直方面的扩展
    * 基于与topic以及第二级messageType订阅消息
    * 基于mysql、文件存储方式多重持久层消息存储
    * 保证可靠异步投递
    * 支持两阶段提交分布式事务

#### 工程结构
    kiteq/
    ├── README.md
    ├── binding           订阅关系管理处理跟ZK的交互
    ├── build.sh          安装脚本
    ├── client            KiteQ的客户端
    ├── doc               文档
    ├── handler           KiteQ所需要的处理Handler
    ├── kite_benchmark.go KiteQ的Benchmark程序
    ├── kiteq.go          KiteQ对外启动入口
    ├── pipe              类似netty的pipeline结构的框架，组织event和handler流转
    ├── protocol          KiteQ的协议包，基于PB和定义的Packet
    ├── remoting          网络层包括remoting-server和client及重连逻辑、客户端管理
    ├── server            KiteQ的Server端组装需要的组件
    ├── stat              状态信息统计
    └── store             KiteQ的存储结构

##### 概念：
    
    * Binding:订阅关系，描述订阅某种消息类型的数据结构
    * Consumer : 消息的消费方
    * Producer : 消息的发送方
    * Topic: 消息的主题比如 Trade则为消息主题，一般可以定义为某种业务类型
    * MessageType: 第二级别的消息类型，比如Trade下存在支付成功的pay-succ-200的消息类型

#### 架构图
  ![image](./doc/kiteq.002.png)

#### Zookeeper数据结构
        KiteServer : /kiteq/server/${topic}/ip:port
        Producer   : /kiteq/pub/${topic}/${groupId}/ip:port
        Consumer   : /kiteq/sub/${topic}/${groupId}-bind/#$data(bind)

##### 流程：
    1. KiteQ启动会将自己可以接受和投递的Topics列表给到zookeeper
    2. KiteQ拉取Zookeeper上的Topics下的订阅关系(Bingding:订阅方推送上来的订阅消息信息)。
    3. Consumer推送自己需要订阅的Topic+messageType的消息的订阅关系(Binding)到Zookeeper
    4. Consumer拉取当前提供推送Topics消息的KiteQ地址列表，并发起TCP长连接
    5. Producer推送自己可以发布消息Topics列表到Zookeeper
    6. Producer拉取当前提供接受Topics消息的KiteQ地址列表，并发起TCP长连接

##### 订阅方式: 
    Direct (直接订阅)： 明确的Topic+MessageType订阅消息
    Regx(正则式订阅):  Topic级别下，对MessageType进行正则匹配方式订阅消息
    Fanout(广播式订阅): Topic级别下，订阅所有的MessageType的消息

#####  两阶段提交：
    因为引入了异步投递方案，所以在有些场景下需要本地执行某个事务成功的时候，本条消息才可以被订阅方消费。
    例如：
        用户购买会员支付成功成功需要修改本地用户账户Mysql的余额、并且告知会员系统为用户的会员期限延长。
        这个时候就会碰到、必须在保证mysql操作成功的情况下，会员系统才可以接收到会员延期的消息。
    
    对于以上的问题，KiteQ的处理和ali的Notify系统一样，
        1. 发送一个UnCommit的消息到KiteQ ,KiteQ 不会对Uncommite的消息做投递操作
        2. KiteQ定期对UnCommit的消息向Producer发送TxAck的询问
        3. 直到Producer明确告诉Commit或者Rollback该消息
        4. Commit会走正常投递流程、Rollback会对当前消息回滚即删除操作。

##### Benchmark
      Env: 4Core * 2 CPU  8G RAM 
      
#####  QuickStart
    1.编译：sh build.sh 
    2.安装装Zookeeper:省略
    启动KiteQ:
        ./kiteq -bind=172.30.3.124:13800 -pport=13801 -db="mock://kiteq" -topics=trade,feed -zkhost=localhost:2181
        -bind  //绑定本地IP:Port
        -pport //pprof的Http端口
        -db //存储的协议地址  mock:// 启动mock模式 mysql:// mmap:// 
        -topics //本机可以处理的topics列表逗号分隔
        -zkhost //zk的地址

    启动客户端：
        对于KiteQClient需要实现消息监听器，我们定义了如下的接口：
        type IListener interface {
            //接受投递消息的回调
            OnMessage(msg *protocol.StringMessage) bool
            //接收事务回调
            // 除非明确提交成功、其余都为不成功
            // 有异常或者返回值为false均为不提交
            OnMessageCheck(tx *protocol.TxResponse) error
        }

    启动Producer :
        producer := client.NewKiteQClient(${zkhost}, ${groupId}, ${password}, &defualtListener{})
        producer.SetTopics([]string{"trade"})
        producer.Start()
        //构建消息
        msg := &protocol.StringMessage{}
        msg.Header = &protocol.Header{
            MessageId:     proto.String(store.MessageId()),
            Topic:         proto.String("trade"),
            MessageType:   proto.String("pay-succ"),
            ExpiredTime:   proto.Int64(time.Now().Unix()),
            DeliveryLimit: proto.Int32(-1),
            GroupId:       proto.String("go-kite-test"),
            Commit:        proto.Bool(true)}
        msg.Body = proto.String("echo")
        //发送消息
        producer.SendStringMessage(msg)

    启动Consumer:
        consumer:= client.NewKiteQClient(${zkhost}, ${groupId}, ${password}, &defualtListener{})
        consumer.SetBindings([]*binding.Binding{
            binding.Bind_Direct("s-mts-test", "trade", "pay-succ", 1000, true),
        })
        consumer.Start()

    就可以完成发布和订阅消息的功能了.....










