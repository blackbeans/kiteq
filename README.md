kiteq
=======

基于go+protolbuff实现的多种持久化方案的mq框架

#### 简介
    * 基于zk维护发送方、订阅方、broker订阅发送关系、支持水平、垂直方面的扩展
    * 基于与topic以及第二级messageType订阅消息
    * 基于mysql、文件存储方式多重持久层消息存储
    * 保证可靠异步投递
    * 支持两阶段提交分布式事务

#### 架构图
  ![image](./doc/arch.png)


##### 概念：
    
    * Binding:订阅关系，描述订阅某种消息类型的数据结构
    * Consumer : 消息的消费方
    * Producer : 消息的发送方
    * Topic: 消息的主题比如 Trade则为消息主题，一般可以定义为某种业务类型
    * MessageType: 第二级别的消息类型，比如Trade下存在支付成功的pay-succ-200的消息类型

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






