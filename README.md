# RocketMQ Flume Source/Sink
基于flume-ng v1.6.0 自定义的对接RocketMQ的Source和Sink，用于数据的输入和输出。

### 准备工作：   
    1：将pom.xml中flume-ng,rocketmq等版本更改为自己开发的版本；   
    2：执行mvn clean install dependency:copy-dependencies；将rocketmq-flume相关jar拷贝到$FLUME_HOME/lib目录中   
    3：启动flume即可   

==========================================================================================================
### Source配置及启动说明：
##### config:
        - topic 指定mq topic, 必填
        - tag 指定mq tag名称, 选填, 默认 *
        - consumerGroup 指定mq consumerGroup, 选填,默认CG_ROCKETMQ_FLUME
        - namesrvAddr 指定RocketMQ的namesrvAddr,选填,优先从config文件获取,如果没指定,则jvm参数必须包含-Drocketmq.namesrv.domain=nsa,否则报错
        - asyn 指定consumer为同步发送还是异步发送,选填,默认true
        - maxSize 缓存消息的大小,>=maxSize则进行process event,选填,默认20
        - maxDelay 缓存消息的延迟时间,>=maxDelay则进行process event,选填,默认2000ms
        - messageModel 消息模式,选填,支持["CLUSTERING"(默认),"BROADCASTING"]
        - consumeFromWhere 设置从何处开始消费,选填,支持["CONSUME_FROM_LAST_OFFSET"(默认),"CONSUME_FROM_FIRST_OFFSET","CONSUME_FROM_TIMESTAMP"]
        - consumeTimestamp 当consumeFromWhere=CONSUME_FROM_TIMESTAMP,指定时间戳,时间精度秒,时间格式"20131223171201",表示2013年12月23日17点12分01秒,选填,默认回溯到相对启动时间的半小时前(RocketMQ支持)
        - extra 可以指定一个extra字段,放入event的headers中,后续进行处理,选填
##### other:
        - rocketmq msg中的properties内容，都会默认放到Flume Event的headers中，另外topic,tag,extra字段也在headers中
##### config demo:
        agent_log.sources = source_rocketmq
        # Descrie the source
        agent_log.sources.source_rocketmq.type = com.ndpmedia.flume.source.rocketmq.RocketMQSource
        agent_log.sources.source_rocketmq.namesrvAddr = 172.30.30.125:9876
        agent_log.sources.source_rocketmq.topic = T_FLUME_NGINX
        agent_log.sources.source_rocketmq.consumerGroup = CG_FLUME_NGINX
        agent_log.sources.source_rocketmq.maxSize = 100
        agent_log.sources.source_rocketmq.maxDelay = 5000
        agent_log.sources.source_rocketmq.messageModel = BROADCASTING
        agent_log.sources.source_rocketmq.consumeFromWhere = CONSUME_FROM_LAST_OFFSET
        agent_log.sources.source_rocketmq.extra = aabbcc
        # Bind the source and sink to the channel
        agent_log.sources.source_rocketmq.channel = ch_mem
##### 启动命令：
        nohup flume-ng agent -c ./conf -f ./conf/rocketmq-fileroll.conf -n agent_log -Denable_ssl=true -Drocketmq.namesrv.domain=rocketmqNameSrv -Dlog.home=/flume-ng/logs -Dflume.root.logger=DEBUG,console &


==============================================================================
### Sink  配置及启动说明：
##### config：####
        - topic 指定mq topic, 必填
        - tag 指定mq tag名称, 选填, 默认*
        - producerGroup 指定 mq producerGroup, 选填,默认PG_ROCKETMQ_FLUME
        - namesrvAddr 指定RocketMQ的namesrvAddr,选填,优先从config文件获取,如果没指定,则jvm参数必须包含-Drocketmq.namesrv.domain=nsa,否则报错
        - allow 指定mq允许发送的过滤消息条件(正则表达式),选填,不填则全部允许
        - deny 指定mq拒绝发送的过滤消息条件(正则表达式),选填,不填则全部允许
        - asyn 指定producer为同步发送还是异步发送,选填,默认true
        - extra 可以指定一个extra字段，放入msg的properties中，后续进行处理,选填,默认空
##### other:
        - flume自带的source interceptor内容，都会默认放到RocketMQ.Message的properties中
##### config demo:
        agent_log.sinks = sink_rocketmq
        # Descrie the sink
        agent_log.sinks.sink_rocketmq.type = com.ndpmedia.flume.sink.rocketmq.RocketMQSink
        agent_log.sinks.sink_rocketmq.namesrvAddr = 172.30.30.125:9876
        agent_log.sinks.sink_rocketmq.topic = T_FLUME_NGINX
        agent_log.sinks.sink_rocketmq.producerGroup = PG_FLUME_NGINX
        agent_log.sinks.sink_rocketmq.allow = ^.*$
        agent_log.sinks.sink_rocketmq.asyn = true
        agent_log.sinks.sink_rocketmq.extra = aabbcc
        # Bind the source and sink to the channel
        agent_log.sinks.sink_rocketmq.channel = ch_mem
##### 启动命令：
        nohup flume-ng agent -c ./conf -f ./conf/exec-rocketmq.conf -n agent_log -Denable_ssl=true -Drocketmq.namesrv.domain=rocketmqNameSrv -Dlog.home=/flume-ng/logs -Dflume.root.logger=DEBUG,console &


===========================================================================
##### 需要拷贝到$FLUME_HOME/lib下的jar包含：
        $PROJECT_HOME/rocketmq-flume-sink/target/rocketmq-flume-sink-1.0-SNAPSHOT.jar
        $PROJECT_HOME/rocketmq-flume-sink/target/dependency/fastjson-1.1.41.jar
        $PROJECT_HOME/rocketmq-flume-sink/target/dependency/netty-all-4.0.23.Final.jar
        $PROJECT_HOME/rocketmq-flume-sink/target/dependency/rocketmq-client-3.2.2.R2.jar
        $PROJECT_HOME/rocketmq-flume-sink/target/dependency/rocketmq-common-3.2.2.R2.jar
        $PROJECT_HOME/rocketmq-flume-sink/target/dependency/rocketmq-remoting-3.2.2.R2.jar
    
