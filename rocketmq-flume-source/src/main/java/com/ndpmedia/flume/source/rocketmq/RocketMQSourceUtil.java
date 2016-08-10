package com.ndpmedia.flume.source.rocketmq;

import com.alibaba.rocketmq.client.consumer.*;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.flume.Context;

public class RocketMQSourceUtil {

    public static DefaultMQPullConsumer getConsumerInstance(Context context) {
        final String consumerGroup = context.getString(RocketMQSourceConstant.CONSUMER_GROUP, RocketMQSourceConstant.DEFAULT_CONSUMER_GROUP);
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        return consumer;
    }
}
