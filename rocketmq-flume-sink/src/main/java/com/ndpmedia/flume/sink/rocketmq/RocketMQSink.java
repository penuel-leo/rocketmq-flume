package com.ndpmedia.flume.sink.rocketmq;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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

    private String extra;

    private boolean asyn = true;//是否异步发送

    @Override public void configure(Context context) {
        // 获取配置项
        topic = context.getString(RocketMQSinkConstant.TOPIC, RocketMQSinkConstant.DEFAULT_TOPIC);
        tag = context.getString(RocketMQSinkConstant.TAG, RocketMQSinkConstant.DEFAULT_TAG);
        // 初始化Producer
        producer = RocketMQSinkUtil.getProducerInstance(context);

        allow = context.getString(RocketMQSinkConstant.ALLOW, null);
        deny = context.getString(RocketMQSinkConstant.DENY, null);
        extra = context.getString(RocketMQSinkConstant.EXTRA,null);

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

            final Message msg = new Message(topic, tag, event.getBody());
            if (null != event.getHeaders() && event.getHeaders().size() > 0 ){
                for ( Map.Entry<String,String> entry : event.getHeaders().entrySet() ){
                    msg.putUserProperty(entry.getKey(),entry.getValue());
                }
            }
            if ( null != extra && extra.length() > 0 ){
                msg.putUserProperty("extra",extra);
            }

            if ( asyn ) {
                producer.send(msg, new SendCallback() {

                    @Override public void onSuccess(SendResult sendResult) {
                        LOG.debug("send success->" + sendResult.getMsgId());
                    }

                    @Override public void onException(Throwable e) {
                        LOG.error("send exception->", e);
                        SendResult result = null;
                        try {
                            result = producer.send(msg);//异步发送失败，会再同步发送一次
                            if ( null == result || result.getSendStatus() != SendStatus.SEND_OK ) {
                                LOG.warn("sync send msg fail:sendResult={}", result);
                            }
                        } catch ( Exception e1 ) {
                            LOG.error("asyn send msg retry fail: sendResult=" + result, e);
                        }
                    }
                });
            } else {
                SendResult sendResult = producer.send(msg); //默认失败会重试
                LOG.debug("sendResult->{}", sendResult);
                if ( null == sendResult || sendResult.getSendStatus() != SendStatus.SEND_OK ) {
                    LOG.warn("sync send msg fail:sendResult={}", sendResult);
                }
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

    public static void main(String[] args) {
        String regex = "^.*/trace.*|.*/conv.*$";
        String msg = "0.002-_-181.58.80.174-_-global.ymtracking.com-_-10.5.10.11:8080-_-302-_-28/Sep/2015:09:13:19 +0000-_-GET " +
                "/conv?offer_id=105495&aff_id=101581&aff_sub=518534_1 HTTP/1.1-_-302-_-278-_---_-Mozilla/5.0 (Linux; U; Android 4.3.3; es-es; M6 Build/JDQ39)" +
                " AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30-_---_-0.002";
        System.out.println(msg.matches(regex));
    }

}
