package com.ndpmedia.flume.source.rocketmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueByDataCenter;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.flume.Context;

public class RocketMQSourceUtil {

    public static DefaultMQPullConsumer getConsumerInstance(Context context) {
        final String consumerGroup = context.getString(RocketMQSourceConstant.CONSUMER_GROUP, RocketMQSourceConstant.DEFAULT_CONSUMER_GROUP);
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);

        String nameSrvAddr = context.getString(RocketMQSourceConstant.NAMESRVADDR);
        if ( null != nameSrvAddr && nameSrvAddr.trim().length() > 0 ) {
            consumer.setNamesrvAddr(nameSrvAddr);
        } else {
            nameSrvAddr = System.getProperty("rocketmq.namesrv.domain", null);
            if ( nameSrvAddr == null || nameSrvAddr.trim().length() == 0 ) {
                nameSrvAddr = "auto fetch"; //这里是因为我厂更改了RocketMQ的namesrv获取方式而自定义的，可忽略
            } else if ( nameSrvAddr.contains(":") ) {//包含port的话，就设置consumer的nameSrvAddr
                consumer.setNamesrvAddr(nameSrvAddr);//from jvm
            } else {//这里是因为我厂更改了RocketMQ的namesrv获取方式而自定义的，可忽略
                System.out.println("------------nameSrvAddr is " + nameSrvAddr + " and not set consumer.namesrvAddr---------------");
            }
        }
        consumer.setMessageModel(MessageModel.valueOf(context.getString(RocketMQSourceConstant.MESSAGE_MODEL, RocketMQSourceConstant.DEFAULT_MESSAGE_MODEL)));
        consumer.changeInstanceNameToPID();
//        MQClientInstance clientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(consumer, null);
        // TODO make consume from where configurable.
//        consumer.setAllocateMessageQueueStrategy(new AllocateMessageQueueByDataCenter(clientInstance));

        consumer.setMessageModel(MessageModel.CLUSTERING);
        return consumer;
    }
}
