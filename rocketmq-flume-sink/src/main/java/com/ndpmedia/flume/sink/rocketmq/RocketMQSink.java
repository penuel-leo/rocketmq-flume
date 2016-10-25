package com.ndpmedia.flume.sink.rocketmq;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.selector.Region;
import com.alibaba.rocketmq.client.producer.selector.SelectMessageQueueByRegion;
import com.alibaba.rocketmq.common.message.Message;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

    private static final int MINIMUM_AWAIT_TIME_IN_SECONDS = 15;

    /**
     * Maximum number of events to handle in a transaction.
     */
    private int batchSize;

    private static Pattern ALLOW_PATTERN = null;

    private static Pattern DENY_PATTERN = null;

    private MessageQueueSelector messageQueueSelector;

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

        batchSize = context.getInteger(RocketMQSinkConstant.SEND_BATCH_SIZE, 128);

        messageQueueSelector = new SelectMessageQueueByRegion(Region.SAME);

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

            for (int i = 0; i < batchSize; i++) {
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
                LOG.info("No qualified events. Backing off.");
                return Status.BACKOFF;
            } else {
                if (asyn) { // send messages asynchronously
                    final CountDownLatch countDownLatch = new CountDownLatch(events.size());
                    final AtomicInteger successCount = new AtomicInteger();
                    for (Event event : events) {
                        final Message msg = wrap(event);
                        try {
                            producer.send(msg, messageQueueSelector, null, new SendCallback() {
                                @Override
                                public void onSuccess(SendResult sendResult) {
                                    successCount.incrementAndGet();
                                    countDownLatch.countDown();
                                    LOG.debug("send success msg:{}, result:{}", msg, sendResult);
                                }

                                @Override
                                public void onException(Throwable throwable) {
                                    LOG.debug("Send message async throws exception: {}", throwable);
                                    try {
                                        SendResult result = producer.send(msg, messageQueueSelector, null);//异步发送失败，会再同步发送一次
                                        successCount.incrementAndGet();
                                        countDownLatch.countDown();
                                        LOG.debug("sync send success. SendResult: {}", result);
                                    } catch (Exception e) {
                                        LOG.error("sync sending message failed too. Mark this batch as failed");
                                        // Fail fast.
                                        drainCountDownLatch(countDownLatch);
                                    }
                                }
                            });
                        } catch (Exception e) {
                            // Fail fast.
                            drainCountDownLatch(countDownLatch);
                            LOG.error("Send message failed.");
                        }
                    }

                    try {
                        countDownLatch.await(Math.max(events.size(), MINIMUM_AWAIT_TIME_IN_SECONDS), TimeUnit.SECONDS);
                        if (successCount.get() >= events.size()) {
                            tx.commit();
                            LOG.debug("Transaction committed.");
                            return Status.READY;
                        } else {
                            tx.rollback();
                            LOG.warn("Transaction rolls back.");
                            return Status.BACKOFF;
                        }
                    } catch (InterruptedException e) {
                        LOG.error("Awaiting thread was interrupted. Possibly reasons are: 1) Bad network; 2) System shut down;");
                        tx.rollback();
                        return Status.BACKOFF;
                    } catch (Exception e) {
                        LOG.error("Unexpected exception on commit/rollback", e);
                        return Status.BACKOFF;
                    }

                } else { // Send message synchronously
                    try {
                        for (Event event : events) {
                            SendResult sendResult = producer.send(wrap(event), messageQueueSelector, null); //默认失败会重试
                            LOG.debug("Send OK. SendResult: {}", sendResult);
                        }
                        tx.commit();
                        return Status.READY;
                    } catch (Exception e) {
                        tx.rollback();
                        LOG.warn("Transaction rolled back");
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

    private void drainCountDownLatch(CountDownLatch countDownLatch) {
        while (countDownLatch.getCount() > 0) {
            countDownLatch.countDown();
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
