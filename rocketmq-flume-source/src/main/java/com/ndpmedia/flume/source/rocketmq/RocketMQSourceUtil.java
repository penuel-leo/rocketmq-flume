package com.ndpmedia.flume.source.rocketmq;

import com.alibaba.rocketmq.client.consumer.*;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.flume.Context;

/**
 * RocketMQSinkUtil Created with rocketmq-flume.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/9/17 上午10:42
 * @desc
 */
public class RocketMQSourceUtil {

    public static MQPushConsumer getConsumerInstance(Context context) {
        final String consumerGroup = context.getString(RocketMQSourceConstant.CONSUMER_GROUP, RocketMQSourceConstant.DEFAULT_CONSUMER_GROUP);
        System.out.println("----------consumerGroup is " + consumerGroup + " -----------");

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);

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
        ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.valueOf(context.getString(RocketMQSourceConstant.CONSUME_FROM_WHERE,
                                                                                       RocketMQSourceConstant.DEFAULT_CONSUME_FROM_WHERE));
        consumer.setConsumeFromWhere(consumeFromWhere);
        if ( consumeFromWhere == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP ) {
            String consumeTimeStamp = context.getString(RocketMQSourceConstant.CONSUME_TIMESTAMP);
            if ( consumeTimeStamp != null && consumeTimeStamp.length() > 0 ) {

                // Validate timestamp format.
                if (null == UtilAll.parseDate(consumeTimeStamp, UtilAll.yyyyMMddHHmmss)) {
                   throw new RuntimeException("Illegal time format");
                }

                consumer.setConsumeTimestamp(consumeTimeStamp);
            }
        }

        int pullBatchSize = context.getInteger(RocketMQSourceConstant.PULL_BATCH_SIZE,
                RocketMQSourceConstant.DEFAULT_PULL_BATCH_SIZE);
        consumer.setPullBatchSize(pullBatchSize);

        int consumeBatchSize = context.getInteger(RocketMQSourceConstant.CONSUME_BATCH_SIZE,
                RocketMQSourceConstant.DEFAULT_CONSUME_BATCH_SIZE);
        consumer.setConsumeMessageBatchMaxSize(consumeBatchSize);

        int corePoolSize = context.getInteger(RocketMQSourceConstant.CORE_POOL_SIZE,
                RocketMQSourceConstant.DEFAULT_CORE_POOL_SIZE);
        consumer.setConsumeThreadMin(corePoolSize);

        int maxPoolSize = context.getInteger(RocketMQSourceConstant.MAX_POOL_SIZE,
                RocketMQSourceConstant.DEFAULT_MAX_POOL_SIZE);
        consumer.setConsumeThreadMax(maxPoolSize);

        System.out.println("----------nameSrvAddr is " + nameSrvAddr + " -----------");
        return consumer;
    }
}
