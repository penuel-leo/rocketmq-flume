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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * RocketMQSink Created with rocketmq-flume.
 *
 * @author penuel (penuel.leo@gmail.com)
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

    private static Pattern ALLOW_PATTERN = null;

    private static Pattern DENY_PATTERN = null;

    @Override
    public void configure(Context context) {
        // 获取配置项
        topic = context.getString(RocketMQSinkConstant.TOPIC, RocketMQSinkConstant.DEFAULT_TOPIC);
        tag = context.getString(RocketMQSinkConstant.TAG, RocketMQSinkConstant.DEFAULT_TAG);
        // 初始化Producer
        producer = RocketMQSinkUtil.getProducerInstance(context);

        allow = context.getString(RocketMQSinkConstant.ALLOW, null);
        if (null != allow && !allow.trim().isEmpty()) {
            ALLOW_PATTERN = Pattern.compile(allow.trim());
        }

        deny = context.getString(RocketMQSinkConstant.DENY, null);

        if (null != deny && !deny.trim().isEmpty()) {
            DENY_PATTERN = Pattern.compile(deny.trim());
        }

        extra = context.getString(RocketMQSinkConstant.EXTRA, null);

        asyn = context.getBoolean(RocketMQSinkConstant.ASYN, true);

        if (LOG.isInfoEnabled()) {
            LOG.info("RocketMQSource configure success, topic={},tag={},allow={},deny={},extra={}, asyn={}", topic, tag, allow, deny, extra, asyn);
        }

    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        try {
            tx.begin();
            List<Event> events = new ArrayList<>();

            while (true) {
                Event event = channel.take();
                if (null == event || null == event.getBody() || 0 == event.getBody().length) {
                    break;
                } else {
                    if (null != ALLOW_PATTERN || null != DENY_PATTERN) {
                        String msg = new String(event.getBody(), "UTF-8");
                        if (null != DENY_PATTERN && DENY_PATTERN.matcher(msg).matches()) {
                            continue;
                        }

                        if (null != ALLOW_PATTERN && !ALLOW_PATTERN.matcher(msg).matches()) {
                            continue;
                        }

                        events.add(event);
                    } else {
                        events.add(event);
                    }
                }
            }

            if (events.isEmpty()) {
                tx.commit();
                return Status.READY;
            } else {
                if (asyn) { // send messages asynchronously
                    final CountDownLatch countDownLatch = new CountDownLatch(events.size());
                    final AtomicBoolean hasError = new AtomicBoolean(false);
                    final Thread thread = Thread.currentThread();
                    for (Event event : events) {
                        final Message msg = wrap(event);
                        try {
                            producer.send(msg, new SendCallback() {
                                @Override
                                public void onSuccess(SendResult sendResult) {
                                    countDownLatch.countDown();
                                    LOG.debug("send success msg:{}, result:{}", msg, sendResult);
                                }

                                @Override
                                public void onException(Throwable throwable) {
                                    LOG.debug("Send message async throws exception: {}", throwable);
                                    try {
                                        SendResult result = producer.send(msg);//异步发送失败，会再同步发送一次
                                        countDownLatch.countDown();
                                        LOG.debug("sync send success. SendResult: {}", result);
                                    } catch (Exception e) {
                                        LOG.error("sync sending message failed too. Mark this batch as failed");
                                        hasError.set(true);
                                        countDownLatch.countDown();
                                        LOG.error("Interrupt the main awaiting thread");
                                        thread.interrupt();
                                    }
                                }
                            });
                        } catch (Exception e) {
                            countDownLatch.countDown();
                            hasError.set(true);
                            LOG.error("Send message failed.");
                        }

                    }

                    try {
                        countDownLatch.await();
                        if (!hasError.get()) {
                            tx.commit();
                            return Status.READY;
                        } else {
                            tx.rollback();
                            return Status.BACKOFF;
                        }
                    } catch (InterruptedException e) {
                        LOG.error("Awaiting thread was interrupted. Possibly reasons are: 1) Bad network; 2) System shut down;");
                        tx.rollback();
                        return Status.BACKOFF;
                    }
                } else { // Send message synchronously
                    try {
                        for (Event event : events) {
                            SendResult sendResult = producer.send(wrap(event)); //默认失败会重试
                            LOG.debug("Send OK. SendResult: {}", sendResult);
                        }
                        tx.commit();
                        return Status.READY;
                    } catch (Exception e) {
                        tx.rollback();
                        return Status.BACKOFF;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Unexpected exception", e);
            try {
                tx.rollback();
            } catch (Exception cause) {
                LOG.error("Rollback exception", cause);
            }
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }

    private Message wrap(Event event) {
        Message msg = new Message(topic, tag, event.getBody());
        if (null != event.getHeaders() && event.getHeaders().size() > 0) {
            for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
                msg.putUserProperty(entry.getKey(), entry.getValue());
            }
        }
        if (null != extra && extra.length() > 0) {
            msg.putUserProperty("extra", extra);
        }

        return msg;
    }

    @Override
    public synchronized void start() {
        try {
            LOG.warn("RocketMQSink start producer... ");
            producer.start();
        } catch (MQClientException e) {
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

}
