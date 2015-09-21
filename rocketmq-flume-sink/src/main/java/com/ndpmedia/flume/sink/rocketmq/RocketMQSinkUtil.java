package com.ndpmedia.flume.sink.rocketmq;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import org.apache.flume.Context;

/**
 * RocketMQSinkUtil Created with rocketmq-flume.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/9/17 上午10:42
 * @desc
 */
public class RocketMQSinkUtil {

    public static MQProducer getProducerInstance(Context context) {
        final String producerGroup = context.getString(RocketMQSinkConstant.PRODUCER_GROUP, RocketMQSinkConstant.DEFAULT_PRODUCER_GROUP);
        System.out.println("----------producerGroup is "+producerGroup+" -----------");

        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);

        String nameSrvAddr = context.getString(RocketMQSinkConstant.NAMESRVADDR);
        if ( null != nameSrvAddr && nameSrvAddr.trim().length() > 0 ){
            producer.setNamesrvAddr(nameSrvAddr);
        }else{
            nameSrvAddr= System.getProperty("rocketmq.namesrv.domain", null);
        }

        System.out.println("----------nameSrvAddr is "+nameSrvAddr+" -----------");
        checkNotNullNorEmpty("nameSrvAddr",nameSrvAddr);

        return producer;
    }

    public static void checkNotNullNorEmpty(String name, String s) {
        if (null == s || s.trim().length() == 0) {
            throw new IllegalArgumentException(name + " should not null nor empty.");
        }
    }

}
