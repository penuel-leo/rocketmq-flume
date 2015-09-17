package com.ndpmedia.flume.sink.rocketmq;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocketMQSink Created with rocketmq-flume.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/9/17 上午10:43
 * @desc
 */
public class RocketMQSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSink.class);

    private String topic;

    private String tag;

    private MQProducer producer;

    private String allow;

    private String deny;

    private boolean asyn = true;//是否异步发送

    @Override public void configure(Context context) {
        // 获取配置项
        topic = context.getString(RocketMQSinkConstant.TOPIC, RocketMQSinkConstant.DEFAULT_TOPIC);
        tag = context.getString(RocketMQSinkConstant.TAG, RocketMQSinkConstant.DEFAULT_TAG);
        // 初始化Producer
        producer = RocketMQSinkUtil.getProducerInstance(context);

        allow = context.getString(RocketMQSinkConstant.ALLOW, null);
        deny = context.getString(RocketMQSinkConstant.DENY, null);

        asyn = context.getBoolean(RocketMQSinkConstant.ASYN, true);

    }

    @Override public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        try {
            tx.begin();
            Event event = channel.take();
            if ( event == null || event.getBody() == null || event.getBody().length == 0 ) {
                tx.commit();
                return Status.READY;
            }

            if ( null != deny && deny.trim().length() > 0 ) {
                String msg = new String(event.getBody(), "UTF-8");
                if ( msg.matches(deny) ) {
                    tx.commit();
                    msg = null;
                    return Status.READY;
                }
            }

            if ( null != allow && allow.trim().length() > 0 ) {
                String msg = new String(event.getBody(), "UTF-8");
                if ( !msg.matches(allow) ) {
                    tx.commit();
                    msg = null;
                    return Status.READY;
                }
            }

            // 发送消息
            if ( asyn ){
                producer.send(new Message(topic, tag, event.getBody()), callback);
            }else {
                SendResult sendResult = producer.send(new Message(topic, tag, event.getBody()));
                LOG.debug("sendResult->{}",sendResult);
            }
            tx.commit();
            return Status.READY;
        } catch ( Exception e ) {
            LOG.error("RocketMQSink send message exception", e);
            try {
                tx.rollback();
                return Status.BACKOFF;
            } catch ( Exception e2 ) {
                LOG.error("Rollback exception", e2);
            }
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }

    @Override
    public synchronized void start() {
        try {
            LOG.warn("RocketMQSink start producer... ");
            producer.start();
        } catch ( MQClientException e ) {
            LOG.error("RocketMQSink start producer failed", e);
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        // 停止Producer
        producer.shutdown();
        super.stop();
        LOG.warn("RocketMQSink stop producer... ");
    }

    SendCallback callback = new SendCallback() {

        @Override public void onSuccess(SendResult sendResult) {
            LOG.debug("send success->" + sendResult.getMsgId());
        }

        @Override public void onException(Throwable e) {
            LOG.error("send exception->", e);

        }
    };
}
