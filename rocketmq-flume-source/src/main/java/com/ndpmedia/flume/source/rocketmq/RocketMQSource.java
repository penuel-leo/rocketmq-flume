package com.ndpmedia.flume.source.rocketmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flume.source.PollableSourceConstants.*;

/**
 * RocketMQSource Created with rocketmq-flume.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/9/17 下午12:13
 * @desc
 */
public class RocketMQSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSource.class);

    private String topic;

    private String tag;

    private DefaultMQPullConsumer consumer;

    private String extra;

    private AtomicReference<Set<MessageQueue>> messageQueues = new AtomicReference<Set<MessageQueue>>();

    private SourceCounter counter;

    private int pullBatchSize;

    private Long maxBackOffSleepInterval;

    private Long backoffSleepIncrement;

    @Override public void configure(Context context) {
        topic = context.getString(RocketMQSourceConstant.TOPIC, RocketMQSourceConstant.DEFAULT_TOPIC);
        tag = context.getString(RocketMQSourceConstant.TAG, RocketMQSourceConstant.DEFAULT_TAG);
        extra = context.getString(RocketMQSourceConstant.EXTRA, "");
        String messageModel = context.getString(RocketMQSourceConstant.MESSAGE_MODEL, RocketMQSourceConstant.DEFAULT_MESSAGE_MODEL);
        String fromWhere = context.getString(RocketMQSourceConstant.CONSUME_FROM_WHERE, RocketMQSourceConstant.DEFAULT_CONSUME_FROM_WHERE);
        pullBatchSize = context.getInteger(RocketMQSourceConstant.PULL_BATCH_SIZE, RocketMQSourceConstant.DEFAULT_PULL_BATCH_SIZE);
        backoffSleepIncrement = context.getLong(BACKOFF_SLEEP_INCREMENT, DEFAULT_BACKOFF_SLEEP_INCREMENT);
        maxBackOffSleepInterval = context.getLong(MAX_BACKOFF_SLEEP, DEFAULT_MAX_BACKOFF_SLEEP);
        consumer = RocketMQSourceUtil.getConsumerInstance(context);

        if ( null == counter ) {
            counter = new SourceCounter(getName());
        }
    }

    private boolean handlePullResult(PullResult pullResult, List<Event> events) {
        Preconditions.checkNotNull(events);
        Preconditions.checkNotNull(pullResult);

        switch ( pullResult.getPullStatus() ) {
        case FOUND:
            List<MessageExt> messages = pullResult.getMsgFoundList();
            LOG.debug("Pulled {} messages", messages.size());
            for ( MessageExt messageExt : messages ) {
                // filter by tag.
                if ( null != tag && !tag.trim().equals("*") ) {
                    if ( !tag.trim().equals(messageExt.getTags()) ) {
                        continue;
                    }
                }
                Event event = new SimpleEvent();
                Map<String, String> headers = new HashMap<String, String>();
                headers.put(RocketMQSourceConstant.TOPIC, topic);
                headers.put(RocketMQSourceConstant.TAG, tag);
                if ( null != extra && extra.length() > 0 ) {
                    headers.put(RocketMQSourceConstant.EXTRA, extra);
                }
                headers.putAll(messageExt.getProperties());
                event.setHeaders(headers);
                event.setBody(messageExt.getBody());
                events.add(event);
            }
            return true;

        case NO_MATCHED_MSG:
            // fall through on purpose.
        case NO_NEW_MSG:
            break;

        case SLAVE_LAG_BEHIND:
            LOG.warn("The master broker is down!!");
            break;

        case SUBSCRIPTION_NOT_LATEST:
            LOG.warn("Subscription is the latest");
            break;

        case OFFSET_ILLEGAL:
            LOG.error("Illegal offset!!");
            break;

        default:
            break;
        }

        return false;
    }

    private void process0(Set<MessageQueue> messageQueues, boolean useLongPull, List<Event> events)
            throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        if ( !useLongPull ) {
            for ( MessageQueue messageQueue : messageQueues ) {
                long offset = consumer.fetchConsumeOffset(messageQueue, false);
                if ( offset < 0 ) {
                    offset = 0;
                }
                boolean needToSwitch;
                do {
                    PullResult pullResult = consumer.pull(messageQueue, tag, offset, pullBatchSize);
                    needToSwitch = !handlePullResult(pullResult, events);
                    if ( !needToSwitch ) {
                        processEvent(events, messageQueue, pullResult.getNextBeginOffset());
                        // Update next offset.
                        offset = pullResult.getNextBeginOffset();
                    }
                } while ( !needToSwitch );
            }
        } else {
            // Randomly choose one message queue and start to long pulling.
            MessageQueue messageQueue = messageQueues.iterator().next();
            long offset = consumer.fetchConsumeOffset(messageQueue, false);
            if ( offset < 0 ) {
                offset = 0;
            }
            PullResult pullResult = consumer.pullBlockIfNotFound(messageQueue, tag, offset, pullBatchSize);
            if ( handlePullResult(pullResult, events) ) {
                processEvent(events, messageQueue, offset);
            }
        }
    }

    private void processEvent(List<Event> events, MessageQueue messageQueue, long offset) throws MQClientException {
        int eventSize = events.size();
        counter.addToEventReceivedCount(eventSize);

        getChannelProcessor().processEventBatch(events);
        consumer.getOffsetStore().updateOffset(messageQueue, offset, true);
        LOG.debug("processEvent and updateRMQOffset true");
        events.clear();

        counter.addToEventAcceptedCount(eventSize);
    }

    @Override public Status process() throws EventDeliveryException {
        try {
            //            startTime = System.currentTimeMillis();
            Set<MessageQueue> messageQueueSet = messageQueues.get();
            if ( null == messageQueueSet || messageQueueSet.isEmpty() ) {
                LOG.warn("Message queues allocated to this client are currently empty");
                return Status.BACKOFF;
            } else {
                List<Event> events = new ArrayList<Event>();
                process0(messageQueueSet, false, events);
                process0(messageQueueSet, true, events);
            }
        } catch ( Exception e ) {
            LOG.error("RocketMQSource process error", e);
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    @Override public long getBackOffSleepIncrement() {
        return backoffSleepIncrement;
    }

    @Override public long getMaxBackOffSleepInterval() {
        return maxBackOffSleepInterval;
    }

    @Override public synchronized void start() {
        try {
            LOG.warn("RocketMQSource start consumer... ");
            consumer.start();
            counter.start();
            consumer.registerMessageQueueListener(topic, new DefaultMessageQueueListener());
            Set<MessageQueue> messageQueueSet = consumer.fetchSubscribeMessageQueues(topic);
            messageQueues.set(messageQueueSet);
            counter.setOpenConnectionCount(messageQueueSet == null ? 0 : messageQueueSet.size());
        } catch ( MQClientException e ) {
            LOG.error("RocketMQSource start consumer failed", e);
        }
        super.start();
    }

    @Override public synchronized void stop() {
        // 停止Producer
        consumer.shutdown();
        counter.stop();
        LOG.warn("RocketMQSource stop consumer {}, Metrics:{} ", getName(), counter);
    }

    class DefaultMessageQueueListener implements MessageQueueListener {

        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            messageQueues.getAndSet(mqDivided);
        }
    }
}
