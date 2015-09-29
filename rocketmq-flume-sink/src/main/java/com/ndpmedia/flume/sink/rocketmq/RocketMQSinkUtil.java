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
            checkNotNullNorEmpty("nameSrvAddr",nameSrvAddr);
            producer.setNamesrvAddr(nameSrvAddr);
        }else{
            nameSrvAddr= System.getProperty("rocketmq.namesrv.domain", null);
            if ( nameSrvAddr == null || nameSrvAddr.trim().length() == 0 ){
                nameSrvAddr = "auto fetch"; //这里是因为我厂更改了RocketMQ的namesrv获取方式而自定义的，可忽略
            }else{
                producer.setNamesrvAddr(nameSrvAddr);//from jvm
            }
        }

        System.out.println("----------nameSrvAddr is "+nameSrvAddr+" -----------");
        return producer;
    }

    public static void checkNotNullNorEmpty(String name, String s) {
        if (null == s || s.trim().length() == 0) {
            throw new IllegalArgumentException(name + " should not null nor empty.");
        }
    }

}
