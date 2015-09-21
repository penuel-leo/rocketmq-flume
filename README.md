# RocketMQ Flume Source/Sink
基于flume-ng v1.6.0 自定义的对接RocketMQ的Source和Sink，用于数据的输入和输出。

### 准备工作：   
    1：将pom.xml中flume-ng,rocketmq等版本更改为自己开发的版本；   
    2：执行mvn clean install dependency:copy-dependencies；将rocketmq-flume相关jar拷贝到$FLUME_HOME/lib目录中   
    3：启动flume即可   

==========================================================================================================
### Source配置及启动说明：
    略



===========================================================================================================
### Sink  配置及启动说明：
#####config：
        - topic 指定mq topic，必填
        - tags 指定mq tag名称，选填
        - namesrvAddr 指定RocketMQ的namesrvAddr，选填，优先从config文件获取，如果没指定，则jvm参数必须包含-Drocketmq.namesrv.domain=nsa,否则报错
        - allow 指定mq允许发送的过滤消息条件(正则表达式)，选填，不填则全部允许
        - deny 指定mq拒绝发送的过滤消息条件(正则表达式)，选填，不填则全部允许；
        - asyn 指定producer为同步发送还是异步发送，选填，默认true
#####config demo:
        agent_log.sources = src_exec
        agent_log.sinks = sink_rocketmq
        agent_log.channels = ch_mem
        # Describe/configure the source
        agent_log.sources.src_exec.type = exec
        agent_log.sources.src_exec.shell = /bin/bash -c
        agent_log.sources.src_exec.command = tail -F access.log
        agent_log.sources.src_exec.threads = 3
        # Descrie the sink
        agent_log.sinks.sink_rocketmq.type = com.ndpmedia.flume.sink.rocketmq.RocketMQSink
        agent_log.sinks.sink_rocketmq.namesrvAddr = 172.30.30.125:9876
        agent_log.sinks.sink_rocketmq.topic = T_FLUME_NGINX
        agent_log.sinks.sink_rocketmq.producerGroup = PG_FLUME_NGINX
        agent_log.sinks.sink_rocketmq.allow = ^.*$
        agent_log.sinks.sink_rocketmq.asyn = true
        # Use a channel which buffers events in memory
        agent_log.channels.ch_mem.type = memory
        agent_log.channels.ch_mem.capacity = 50
        agent_log.channels.ch_mem.transactionCapacity = 50
        # Bind the source and sink to the channel
        agent_log.sources.src_exec.channels = ch_mem
        agent_log.sinks.sink_rocketmq.channel = ch_mem
#####启动命令：
        nohup flume-ng agent -c ./conf -f ./conf/exec-rocketmq.conf -n agent_log -Denable_ssl=true -Drocketmq.namesrv.domain=rocketmqNameSrv -Dlog.home=/flume-ng/logs -Dflume.root.logger=DEBUG,console &


#####需要拷贝到$FLUME_HOME/lib下的jar包含：
        $PROJECT_HOME/rocketmq-flume-sink/target/rocketmq-flume-sink-1.0-SNAPSHOT.jar
        $PROJECT_HOME/rocketmq-flume-sink/target/dependency/fastjson-1.1.41.jar
        $PROJECT_HOME/rocketmq-flume-sink/target/dependency/netty-all-4.0.23.Final.jar
        $PROJECT_HOME/rocketmq-flume-sink/target/dependency/rocketmq-client-3.2.2.R2.jar
        $PROJECT_HOME/rocketmq-flume-sink/target/dependency/rocketmq-common-3.2.2.R2.jar
        $PROJECT_HOME/rocketmq-flume-sink/target/dependency/rocketmq-remoting-3.2.2.R2.jar
    
