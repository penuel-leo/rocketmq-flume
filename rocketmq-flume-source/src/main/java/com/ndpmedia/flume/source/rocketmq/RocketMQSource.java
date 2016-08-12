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
 * Implement a pull-based source model.
 */
public class RocketMQSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSource.class);

    private String topic;

    private String tag;

    private DefaultMQPullConsumer consumer;

    private String extra;

    private AtomicReference<Set<MessageQueue>> messageQueues = new AtomicReference<>();

    private int pullBatchSize;

    private static final int CONSUME_BATCH_SIZE = 100;

    private static final int WATER_MARK_LOW = 2000;

    private static final int WATER_MARK_HIGH = 8000;

    private static final long DELAY_INTERVAL_ON_EXCEPTION = 3000;

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
            consumer.registerMessageQueueListener(topic, new FlumeMessageQueueListener(consumer));
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
        private final DefaultMQPullConsumer pullConsumer;

        FlumeMessageQueueListener(DefaultMQPullConsumer pullConsumer) {
            this.pullConsumer = pullConsumer;
        }

        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            boolean logRebalanceEvent = false;
            Set<MessageQueue> previous = messageQueues.getAndSet(mqDivided);
            if (null != previous) {
                for (MessageQueue messageQueue : previous) {
                    if (!mqDivided.contains(messageQueue)) {
                        cache.remove(messageQueue);
                        windows.remove(messageQueue);
                        logRebalanceEvent = true;
                        LOG.info("Remove message queue: {}", messageQueue.toString());
                    }

                    if (!mqAll.contains(messageQueue)) {
                        logRebalanceEvent = true;
                        LOG.warn("Message Queue {} is not among known queues, maybe one or more brokers is down",
                                messageQueue.toString());
                    }
                }
            }

            for (MessageQueue messageQueue : mqDivided) {
                if (null == previous || !previous.contains(messageQueue)) {
                    cache.putIfAbsent(messageQueue, new ConcurrentSkipListSet<>(new MessageComparator()));
                    windows.putIfAbsent(messageQueue, new ConcurrentSkipListSet<Long>());
                    logRebalanceEvent = true;

                    long consumeOffset = -1;
                    for (int i = 0; i < 5; ) {
                        try {
                            consumeOffset = pullConsumer.fetchConsumeOffset(messageQueue, true);
                            break;
                        } catch (Throwable e) {
                            ++i;
                            LOG.error("Failed to fetchConsumeOffset {} time(s)", i);
                            LOG.error("Exception Stack Trace", e);
                        }
                    }

                    FlumePullRequest request = new FlumePullRequest(messageQueue, tag,
                            consumeOffset < 0 ? 0 : consumeOffset, // consume offset
                            pullBatchSize);
                    executePullRequest(request);

                    LOG.info("Add message queue: {}", messageQueue.toString());
                }
            }
            if (logRebalanceEvent) {
                LOG.warn("Rebalance just happened!!!");
                LOG.debug("Current consuming the following message queues:");
                int index = 0;
                for (MessageQueue messageQueue : mqDivided) {
                    LOG.debug((index++) + ": " +  messageQueue.toString());
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
            if (offset < 0) {
                this.offset = 0;
            } else {
                this.offset = offset;
            }
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

        private final DefaultMQPullConsumer pullConsumer;

        private final FlumePullRequest flumePullRequest;

        FlumePullTask(DefaultMQPullConsumer pullConsumer, FlumePullRequest flumePullRequest) {
            this.pullConsumer = pullConsumer;
            this.flumePullRequest = flumePullRequest;
        }

        @Override
        public void run() {
            try {
                LOG.debug("Begin to pull message queue: {}, tag: {}, beginOffset: {}, pullBatchSize: {}",
                        flumePullRequest.getMessageQueue().toString(),
                        flumePullRequest.getSubscription(),
                        flumePullRequest.getOffset(),
                        flumePullRequest.getBatchSize());

                pullConsumer.pullBlockIfNotFound(
                        flumePullRequest.getMessageQueue(),
                        flumePullRequest.getSubscription(),
                        flumePullRequest.getOffset(),
                        flumePullRequest.getBatchSize(),
                        new FlumePullCallback(pullConsumer, flumePullRequest.getMessageQueue(), flumePullRequest));
            } catch (Throwable e) {
                LOG.error("Failed to pull", e);
            }
        }
    }

    private void executePullRequest(FlumePullRequest flumePullRequest) {
        pullThreadPool.submit(new FlumePullTask(consumer, flumePullRequest));
    }

    private void executePullRequest(FlumePullRequest flumePullRequest, long delayInterval) {
        pullThreadPool.schedule(new FlumePullTask(consumer, flumePullRequest), delayInterval, TimeUnit.MILLISECONDS);
    }

    private void resume() {
        if (suspendedQueues.isEmpty()) {
            return;
        }

        for (Map.Entry<MessageQueue, Long> next : suspendedQueues.entrySet()) {

            if (!messageQueues.get().contains(next.getKey())) { // Skip those queues that has been reassigned.
                continue;
            }

            FlumePullRequest request = new FlumePullRequest(next.getKey(), tag, next.getValue(), pullBatchSize);
            executePullRequest(request);
            suspendedQueues.remove(next.getKey());
        }
    }

    private class FlumePullCallback implements PullCallback {

        private final DefaultMQPullConsumer pullConsumer;

        private final MessageQueue messageQueue;

        private final FlumePullRequest flumePullRequest;

        private long nextBeginOffset;

        FlumePullCallback(DefaultMQPullConsumer pullConsumer, MessageQueue messageQueue, FlumePullRequest flumePullRequest) {
            this.pullConsumer = pullConsumer;
            this.messageQueue = messageQueue;
            this.flumePullRequest = flumePullRequest;
        }

        @Override
        public void onSuccess(PullResult pullResult) {
            boolean dropped = false;
            try {
                LOG.debug("Pull success, begin to process pull result. message queue: {}", messageQueue.toString());
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        if (messageQueues.get().contains(messageQueue)) {
                            if (!cache.containsKey(messageQueue)) {
                                cache.putIfAbsent(messageQueue, new ConcurrentSkipListSet<>(new MessageComparator()));
                                windows.putIfAbsent(messageQueue, new ConcurrentSkipListSet<Long>());
                            }

                            List<MessageExt> messages = pullResult.getMsgFoundList();
                            cache.get(messageQueue).addAll(messages);
                            LOG.debug("Pulled {} messages to local cache", messages.size());
                        } else {
                            dropped = true;
                            LOG.warn("Drop pulled message as message queue: {} has been reassigned to other clients", messageQueue.toString());
                            return;
                        }
                        nextBeginOffset = pullResult.getNextBeginOffset();
                        break;

                    case NO_MATCHED_MSG:
                        LOG.debug("No matched message found");
                        nextBeginOffset = pullResult.getNextBeginOffset();
                        break;

                    case NO_NEW_MSG:
                        LOG.debug("No new message");
                        nextBeginOffset = pullResult.getNextBeginOffset();
                        break;

                    case OFFSET_ILLEGAL: // Correct offset.
                        for (int i = 0; i < 5; i++) {
                            try {
                                nextBeginOffset =pullConsumer.fetchConsumeOffset(messageQueue, true);
                                break;
                            } catch (Throwable e) {
                                LOG.error("Failed to fetch consume offset {} time(s)", (i+1));
                            }
                        }

                        long max = -1;
                        for (int i = 0; i < 5; i++) {
                            try {
                                max = pullConsumer.maxOffset(messageQueue);
                                break;
                            } catch (Throwable e) {
                                LOG.error("Failed to fetch max offset {} times", (i+1));
                            }
                        }

                        if (nextBeginOffset > max) {
                            nextBeginOffset = max;
                        } else {

                            long min = - 1;
                            for (int i = 0; i < 5; i++) {
                                try {
                                    min = pullConsumer.minOffset(messageQueue);
                                    break;
                                } catch (Throwable e) {
                                    LOG.error("Failed to fetch min offset {} times", (i+1));
                                }
                            }

                            if (nextBeginOffset < min) {
                                nextBeginOffset = min;
                            }
                        }
                        break;

                    default:
                        nextBeginOffset = pullResult.getNextBeginOffset();
                        LOG.error("Error status: {}", pullResult.getPullStatus().toString());
                        break;
                }

            } catch (Throwable e) {
                LOG.error("Failed to process pull result");
            } finally {
                if (!dropped) {
                    if (countOfBufferedMessages() > WATER_MARK_HIGH) {
                        suspendedQueues.put(messageQueue, pullResult.getNextBeginOffset());
                    } else {
                        FlumePullRequest request = new FlumePullRequest(messageQueue, tag, nextBeginOffset, pullBatchSize);
                        executePullRequest(request);
                    }
                }
            }
        }

        @Override
        public void onException(Throwable e) {
            LOG.error("Pull failed", e);
            FlumePullRequest request = new FlumePullRequest(messageQueue, tag, flumePullRequest.getOffset() , pullBatchSize);
            if (countOfBufferedMessages() > WATER_MARK_HIGH) {
                suspendedQueues.put(messageQueue, flumePullRequest.getOffset());
            } else {
                executePullRequest(request, DELAY_INTERVAL_ON_EXCEPTION);
            }
        }
    }

    /**
     * Compare messages pulled from same message queue according to queue offset.
     */
    private class MessageComparator implements Comparator<MessageExt> {
        @Override
        public int compare(MessageExt lhs, MessageExt rhs) {
            return Long.compare(lhs.getQueueOffset(), rhs.getQueueOffset());
        }
    }

}
