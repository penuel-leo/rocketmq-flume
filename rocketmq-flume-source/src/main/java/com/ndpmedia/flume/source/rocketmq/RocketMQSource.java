package com.ndpmedia.flume.source.rocketmq;

import com.alibaba.rocketmq.client.consumer.*;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

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

    private int pullBatchSize;

    private static final int CONSUME_BATCH_SIZE = 100;

    private static final int WATER_MARK_LOW = 2000;

    private static final int WATER_MARK_HIGH = 8000;

    private ConcurrentHashMap<MessageQueue, Long> suspendedQueues = new ConcurrentHashMap<MessageQueue, Long>();

    private final ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<MessageExt>> cache = new ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<MessageExt>>();
    private final ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<Long>> windows = new ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<Long>>();
    private final ScheduledExecutorService pullThreadPool = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void configure(Context context) {
        topic = context.getString(RocketMQSourceConstant.TOPIC, RocketMQSourceConstant.DEFAULT_TOPIC);
        tag = context.getString(RocketMQSourceConstant.TAG, RocketMQSourceConstant.DEFAULT_TAG);
        extra = context.getString(RocketMQSourceConstant.EXTRA, null);
        pullBatchSize = context.getInteger(RocketMQSourceConstant.PULL_BATCH_SIZE, RocketMQSourceConstant.DEFAULT_PULL_BATCH_SIZE);
        consumer = RocketMQSourceUtil.getConsumerInstance(context);

    }

    private boolean hasPendingMessage() {

        Set<MessageQueue> messageQueueSet = messageQueues.get();
        if (null == messageQueueSet || messageQueueSet.isEmpty()) {
            return false;
        }

        for (Map.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>> next : cache.entrySet()) {
            if (!next.getValue().isEmpty()) {
                return true;
            }
        }

        return false;
    }

    private int countOfBufferedMessages() {
        Set<MessageQueue> messageQueueSet = messageQueues.get();
        if (null == messageQueueSet || messageQueueSet.isEmpty()) {
            return 0;
        }

        int count = 0;
        for (Map.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>> next : cache.entrySet()) {
            if (!next.getValue().isEmpty()) {
                count += next.getValue().size();
            }
        }

        return count;
    }

    private Event wrap(MessageExt messageExt) {
        Event event = new SimpleEvent();
        Map<String, String> headers = new HashMap<String, String>();
        headers.put(RocketMQSourceConstant.TOPIC, topic);
        headers.put(RocketMQSourceConstant.TAG, tag);
        headers.put(RocketMQSourceConstant.EXTRA, extra);
        headers.putAll(messageExt.getProperties());
        event.setHeaders(headers);
        event.setBody(messageExt.getBody());
        return event;
    }

    private List<Event> take() {
        List<Event> events = new ArrayList<Event>();
        int count = 0;
        for (Map.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>> next : cache.entrySet()) {
            if (!next.getValue().isEmpty()) {
                MessageExt messageExt;
                while (count < CONSUME_BATCH_SIZE && (messageExt = next.getValue().pollFirst()) != null) {
                    count++;
                    events.add(wrap(messageExt));
                    ConcurrentSkipListSet<Long> window = windows.get(next.getKey());
                    if (null != window) {
                        window.add(messageExt.getQueueOffset());
                    }
                }
            }

            if (count >= CONSUME_BATCH_SIZE) {
                break;
            }
        }

        if (countOfBufferedMessages() < WATER_MARK_LOW) {
            resume();
        }

        return events;
    }

    private void updateOffset() {
        OffsetStore offsetStore = consumer.getOffsetStore();
        Set<MessageQueue> updated = new HashSet<MessageQueue>();
        // calculate offsets according to consuming windows.
        for (ConcurrentHashMap.Entry<MessageQueue, ConcurrentSkipListSet<Long>> entry : windows.entrySet()) {
            while (!entry.getValue().isEmpty()) {

                long offset = offsetStore.readOffset(entry.getKey(), ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                if (offset + 1 > entry.getValue().first()) {
                    entry.getValue().pollFirst();
                } else if (offset + 1 == entry.getValue().first()) {
                    entry.getValue().pollFirst();
                    offsetStore.updateOffset(entry.getKey(), offset + 1, true);
                    updated.add(entry.getKey());
                } else {
                    break;
                }

            }
        }
        offsetStore.persistAll(updated);
    }

    @Override
    public Status process() throws EventDeliveryException {
        try {
            if (hasPendingMessage()) {
                List<Event> events = take();
                if (null != events && !events.isEmpty()) {
                    getChannelProcessor().processEventBatch(events);
                    updateOffset();
                } else {
                    throw new IllegalStateException("Should not happen");
                }
            } else {
                LOG.info("No pending messages to process");
                return Status.BACKOFF;
            }
        } catch (Exception e) {
            LOG.error("RocketMQSource process error", e);
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    @Override
    public synchronized void start() {
        try {
            LOG.warn("RocketMQSource start consumer... ");
            consumer.registerMessageQueueListener(topic, new FlumeMessageQueueListener());
            consumer.start();
        } catch (MQClientException e) {
            LOG.error("RocketMQSource start consumer failed", e);
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        // 停止Producer
        consumer.shutdown();
        super.stop();
        LOG.warn("RocketMQSource stop consumer... ");
    }

    private class FlumeMessageQueueListener implements MessageQueueListener {
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            Set<MessageQueue> previous = messageQueues.getAndSet(mqDivided);
            if (null != previous) {
                for (MessageQueue messageQueue : previous) {
                    if (!mqDivided.contains(messageQueue)) {
                        cache.remove(messageQueue);
                        windows.remove(messageQueue);
                        LOG.info("Remove message queue: {}", messageQueue.toString());
                    }
                }
            }

            for (MessageQueue messageQueue : mqDivided) {
                if (null != previous && !previous.contains(messageQueue)) {
                    cache.put(messageQueue, new ConcurrentSkipListSet<MessageExt>(new MessageComparator()));
                    windows.put(messageQueue, new ConcurrentSkipListSet<Long>());
                    LOG.info("Add message queue: {}", messageQueue.toString());
                }
            }
        }
    }

    private class FlumePullRequest {
        private final MessageQueue messageQueue;
        private final String subscription;
        private final long offset;
        private final int batchSize;

        FlumePullRequest(MessageQueue messageQueue, String subscription, long offset, int batchSize) {
            this.messageQueue = messageQueue;
            this.subscription = subscription;
            this.offset = offset;
            this.batchSize = batchSize;
        }

        MessageQueue getMessageQueue() {
            return messageQueue;
        }

        long getOffset() {
            return offset;
        }

        int getBatchSize() {
            return batchSize;
        }

        String getSubscription() {
            return subscription;
        }
    }

    private class FlumePullTask implements Runnable {

        private final MQPullConsumer pullConsumer;

        private final FlumePullRequest flumePullRequest;

        FlumePullTask(MQPullConsumer pullConsumer, FlumePullRequest flumePullRequest) {
            this.pullConsumer = pullConsumer;
            this.flumePullRequest = flumePullRequest;
        }

        @Override
        public void run() {
            try {
                pullConsumer.pullBlockIfNotFound(
                        flumePullRequest.getMessageQueue(),
                        flumePullRequest.getSubscription(),
                        flumePullRequest.getOffset(),
                        flumePullRequest.getBatchSize(),
                        new FlumePullCallback(flumePullRequest.getMessageQueue(), flumePullRequest));
            } catch (Throwable e) {
                LOG.error("Failed to pull", e);
            }
        }
    }

    private void executePullRequest(FlumePullRequest flumePullRequest) {
        pullThreadPool.execute(new FlumePullTask(consumer, flumePullRequest));
    }

    private void resume() {
        if (suspendedQueues.isEmpty()) {
            return;
        }

        for (Map.Entry<MessageQueue, Long> next : suspendedQueues.entrySet()) {
            FlumePullRequest request = new FlumePullRequest(next.getKey(), tag, next.getValue(), pullBatchSize);
            executePullRequest(request);
            suspendedQueues.remove(next.getKey());
        }
    }

    private class FlumePullCallback implements PullCallback {

        private final MessageQueue messageQueue;

        private final FlumePullRequest flumePullRequest;

        FlumePullCallback(MessageQueue messageQueue, FlumePullRequest flumePullRequest) {
            this.messageQueue = messageQueue;
            this.flumePullRequest = flumePullRequest;
        }

        @Override
        public void onSuccess(PullResult pullResult) {
            try {
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        if (cache.containsKey(messageQueue)) {
                            cache.get(messageQueue).addAll(pullResult.getMsgFoundList());
                        } else {
                            LOG.warn("Drop pulled message as message queue: {} has been reassigned to other clients", messageQueue.toString());
                        }
                        break;

                    case NO_MATCHED_MSG:
                        LOG.debug("No matched message found");
                        break;

                    default:
                        LOG.error("Error status: {}", pullResult.getPullStatus().toString());
                        break;
                }

            } catch (Throwable e) {
                LOG.error("Failed to process pull result");
            } finally {
                if (countOfBufferedMessages() > WATER_MARK_HIGH) {
                    suspendedQueues.put(messageQueue, pullResult.getNextBeginOffset());
                } else {
                    FlumePullRequest request = new FlumePullRequest(messageQueue, tag, pullResult.getNextBeginOffset(), pullBatchSize);
                    executePullRequest(request);
                }
            }
        }

        @Override
        public void onException(Throwable e) {
            LOG.error("Pull message failed", e);
            FlumePullRequest request = new FlumePullRequest(messageQueue, tag, flumePullRequest.getOffset() , pullBatchSize);
            if (countOfBufferedMessages() > WATER_MARK_HIGH) {
                suspendedQueues.put(messageQueue, flumePullRequest.getOffset());
            } else {
                executePullRequest(request);
            }
        }
    }

    /**
     * Compare messages pulled from same message queue according to queue offset.
     */
    private class MessageComparator implements Comparator<MessageExt> {
        @Override
        public int compare(MessageExt lhs, MessageExt rhs) {
            return lhs.getQueueOffset() < rhs.getQueueOffset() ? -1 : (lhs.getQueueOffset() == rhs.getQueueOffset() ? 0 : 1);
        }
    }

}
