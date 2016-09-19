package com.ndpmedia.flume.source.rocketmq;

import com.alibaba.rocketmq.client.ResetOffsetCallback;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Implement a pull-based source model.
 */
public class RocketMQSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSource.class);

    private String topic;

    private String tag;

    private DefaultMQPullConsumer consumer;

    private String extra;

    private ConcurrentHashMap<MessageQueue, ProcessQueue> processMap = new ConcurrentHashMap<>();

    private int pullBatchSize;

    private static final int CONSUME_BATCH_SIZE = 100;

    private static final long DELAY_INTERVAL_ON_EXCEPTION = 3000;

    private final ScheduledExecutorService executorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RocketMQFlumeSourceThread_"));

    private final ConcurrentHashMap<MessageQueue, Long> resetOffsetTable = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<MessageQueue, FlumePullRequest> flowControlMap = new ConcurrentHashMap<>();

    @Override
    public void configure(Context context) {
        topic = context.getString(RocketMQSourceConstant.TOPIC, RocketMQSourceConstant.DEFAULT_TOPIC);
        tag = context.getString(RocketMQSourceConstant.TAG, RocketMQSourceConstant.DEFAULT_TAG);
        extra = context.getString(RocketMQSourceConstant.EXTRA, null);
        pullBatchSize = context.getInteger(RocketMQSourceConstant.PULL_BATCH_SIZE, RocketMQSourceConstant.DEFAULT_PULL_BATCH_SIZE);
        consumer = RocketMQSourceUtil.getConsumerInstance(context);
        consumer.setResetOffsetCallback(new FlumeResetOffsetCallback(consumer));
    }

    private Event wrap(MessageExt messageExt) {
        Event event = new SimpleEvent();
        Map<String, String> headers = new HashMap<>();
        headers.put(RocketMQSourceConstant.TOPIC, topic);
        headers.put(RocketMQSourceConstant.TAG, tag);
        headers.put(RocketMQSourceConstant.EXTRA, extra);
        headers.putAll(messageExt.getProperties());
        event.setHeaders(headers);
        event.setBody(messageExt.getBody());
        return event;
    }

    @Override
    public Status process() throws EventDeliveryException {
        try {

            for (Map.Entry<MessageQueue, ProcessQueue> entry : processMap.entrySet()) {
                MessageQueue messageQueue = entry.getKey();
                ProcessQueue processQueue = entry.getValue();
                if (processQueue.hasPendingMessage()) {
                    List<Event> events = new ArrayList<>();
                    List<MessageExt> messageLists = processQueue.peek(CONSUME_BATCH_SIZE);
                    for (MessageExt message : messageLists) {
                        events.add(wrap(message));
                    }
                    getChannelProcessor().processEventBatch(events);
                    boolean throttling = processQueue.needFlowControl();
                    processQueue.ack(messageLists);

                    if (!messageLists.isEmpty()) {
                        consumer.getOffsetStore().updateOffset(messageQueue, processQueue.getAckOffset(), true);
                    }

                    if (throttling && !processQueue.needFlowControl()) {
                        FlumePullRequest flumePullRequest = flowControlMap.get(messageQueue);
                        if (null == flumePullRequest) {
                            LOG.error("Flow control map should contain the pull request under flow control");
                            flumePullRequest = new FlumePullRequest(messageQueue, tag, processQueue.getMaxOffset(),
                                    pullBatchSize);
                        }
                        executePullRequest(flumePullRequest);
                    }

                    return Status.READY;
                } else {
                    if (!processQueue.isPullAlive()) {
                        LOG.warn("Pulling [{}] has been inactive for more than 10 minutes", messageQueue);
                        processQueue.refreshLastPullTimestamp();
                        FlumePullRequest flumePullRequest = new FlumePullRequest(messageQueue, tag,
                                processQueue.getAckOffset(), pullBatchSize);
                        executePullRequest(flumePullRequest);
                    }
                }
            }

            return Status.BACKOFF;
        } catch (Exception e) {
            LOG.error("RocketMQSource process error", e);
            return Status.BACKOFF;
        }
    }

    @Override
    public synchronized void start() {
        try {
            LOG.info("RocketMQSource start consumer... ");
            consumer.registerMessageQueueListener(topic, new FlumeMessageQueueListener(consumer));
            consumer.start();
            registerWatchDog();
            startPersistOffsetService();
        } catch (MQClientException e) {
            LOG.error("RocketMQSource start consumer failed", e);
        }
        super.start();
    }

    /**
     * This method periodically 1) check and remove message queue marked dropped; 2) resume pulling for those being
     * inactive for more than 10 minutes.
     */
    private void registerWatchDog() {
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    for (Map.Entry<MessageQueue, ProcessQueue> next : processMap.entrySet()) {
                        if (next.getValue().isDropped()) {
                            processMap.remove(next.getKey());
                            LOG.info("Message Queue: [{}] has been marked dropped, remove it from process map.",
                                    next.getKey());
                            continue;
                        }

                        if (!next.getValue().isPullAlive()) {
                            LOG.warn("Message Queue: [{}] has not been pulled for 10 minutes. Resume it now.",
                                    next.getKey());
                            FlumePullRequest flumePullRequest = new FlumePullRequest(next.getKey(), tag,
                                    next.getValue().getAckOffset(), pullBatchSize);
                            executePullRequest(flumePullRequest);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Unexpected exception", e);
                }
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    /**
     * Persist consume offset to remote broker every 5 minutes.
     */
    private void startPersistOffsetService() {
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    for (Map.Entry<MessageQueue, ProcessQueue> next : processMap.entrySet()) {
                        if (!next.getValue().isConsumeOffsetPersisted()) {
                            try {
                                consumer.getOffsetStore().persist(next.getKey());
                                LOG.debug("Offset Persisted. Message Queue: {}, Consume Offset: {}", next.getKey(),
                                        next.getValue().getAckOffset());
                            } catch (Exception e) {
                                LOG.warn("Failed to persist consume offset. Message Queue: {}, Offset: {}",
                                        next.getKey(), next.getValue().getAckOffset());
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Unexpected exception", e);
                }
            }
        }, 300, 300, TimeUnit.SECONDS);
    }

    @Override
    public synchronized void stop() {
        // 停止Producer
        consumer.shutdown();
        executorService.shutdown();
        super.stop();
        LOG.info("RocketMQSource stop consumer... ");
    }

    private class FlumeMessageQueueListener implements MessageQueueListener {
        private final DefaultMQPullConsumer pullConsumer;

        FlumeMessageQueueListener(DefaultMQPullConsumer pullConsumer) {
            this.pullConsumer = pullConsumer;
        }

        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            boolean logRebalanceEvent = false;
            Set<MessageQueue> previous = processMap.keySet();
            for (MessageQueue messageQueue : previous) {
                if (!mqDivided.contains(messageQueue)) {
                    processMap.get(messageQueue).setDropped(true);
                    logRebalanceEvent = true;
                    LOG.info("Remove message queue: {}", messageQueue.toString());
                }

                if (!mqAll.contains(messageQueue)) {
                    logRebalanceEvent = true;
                    LOG.warn("Message Queue {} is not among known queues, maybe one or more brokers is down",
                            messageQueue.toString());
                }
            }

            for (MessageQueue messageQueue : mqDivided) {
                if (!previous.contains(messageQueue)) {
                    processMap.put(messageQueue, new ProcessQueue(messageQueue));
                    logRebalanceEvent = true;

                    long consumeOffset = -1;
                    int i = 0;
                    Throwable cause = null;
                    for (; i < 5; i++) {
                        try {
                            consumeOffset = pullConsumer.fetchConsumeOffset(messageQueue, true);
                            processMap.get(messageQueue).setAckOffset(consumeOffset < 0 ? 0 : consumeOffset);
                            break;
                        } catch (Throwable e) {
                            cause = e;
                        }
                    }

                    if (i >= 5) {
                        LOG.error("Failed to fetchConsumeOffset after attempting {} time(s)", i);
                        LOG.error("Exception Stack Trace", cause);
                    }

                    FlumePullRequest request = new FlumePullRequest(messageQueue, tag,
                            consumeOffset < 0 ? 0 : consumeOffset, // consume offset
                            pullBatchSize);
                    executePullRequest(request);

                    LOG.info("Add message queue: {}", messageQueue.toString());
                }
            }

            if (logRebalanceEvent) {
                LOG.debug("Rebalance just happened!!!");
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
        private long offset;
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

        void setOffset(long offset) {
            this.offset = offset;
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
        executePullRequest(flumePullRequest, 0);
    }

    private void executePullRequest(FlumePullRequest flumePullRequest, long delayIntervalInMilliSeconds) {

        if (resetOffsetTable.containsKey(flumePullRequest.getMessageQueue())) {
            flumePullRequest.setOffset(resetOffsetTable.get(flumePullRequest.getMessageQueue()));

            // Remove after use.
            resetOffsetTable.remove(flumePullRequest.getMessageQueue());
        }

        if (delayIntervalInMilliSeconds > 0) {
            executorService.schedule(new FlumePullTask(consumer, flumePullRequest), delayIntervalInMilliSeconds, TimeUnit.MILLISECONDS);
        } else {
            executorService.submit(new FlumePullTask(consumer, flumePullRequest));
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
            try {
                LOG.debug("Pull success, begin to process pull result. message queue: {}", messageQueue.toString());
                ProcessQueue processQueue = processMap.get(messageQueue);
                if (null == processQueue || processQueue.isDropped()) {
                    return;
                }

                // Refresh last pull timestamp.
                processQueue.refreshLastPullTimestamp();

                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        processQueue.putMessages(pullResult.getMsgFoundList());
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

                        // Take offset suggested by broker as next begin offset
                        nextBeginOffset = pullResult.getNextBeginOffset();

                        LOG.error("Begin to correct offset");
                        // Try very hard to correct offset
                        boolean correctOffsetSuccessful = false;
                        for (int i = 0; i < 5; i++) {
                            try {
                                pullConsumer.getOffsetStore().updateOffset(messageQueue, nextBeginOffset, false);
                                pullConsumer.getOffsetStore().persist(messageQueue);
                                correctOffsetSuccessful = true;
                                LOG.error("Correct offset OK");
                                break;
                            } catch (Throwable ignore) {
                            }
                        }

                        if (!correctOffsetSuccessful) {
                            LOG.error("Correct illegal offset failed");
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
                FlumePullRequest request = new FlumePullRequest(messageQueue, tag, nextBeginOffset, pullBatchSize);
                if (!processMap.get(messageQueue).needFlowControl()) {
                    executePullRequest(request);
                } else {
                    flowControlMap.put(messageQueue, request);
                }
            }
        }

        @Override
        public void onException(Throwable e) {
            LOG.error("Pull failed", e);
            ProcessQueue processQueue = processMap.get(messageQueue);
            if (null != processQueue) {
                processQueue.refreshLastPullTimestamp();
            }

            FlumePullRequest request = new FlumePullRequest(messageQueue, tag, flumePullRequest.getOffset(),
                    pullBatchSize);
            executePullRequest(request, DELAY_INTERVAL_ON_EXCEPTION);
        }
    }

    private class FlumeResetOffsetCallback implements ResetOffsetCallback {

        private final DefaultMQPullConsumer defaultMQPullConsumer;

        private FlumeResetOffsetCallback(DefaultMQPullConsumer defaultMQPullConsumer) {
            this.defaultMQPullConsumer = defaultMQPullConsumer;
        }

        @Override
        public void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {

            for (Map.Entry<MessageQueue, Long> next : offsetTable.entrySet()) {
                resetOffsetTable.put(next.getKey(), next.getValue());
            }

            /*
             * Updating offsets to broker might be time consuming.
             */
            for (Map.Entry<MessageQueue, Long> next : offsetTable.entrySet()) {
                try {
                    defaultMQPullConsumer.getOffsetStore().updateOffset(next.getKey(), next.getValue(), false);
                    defaultMQPullConsumer.getOffsetStore().persist(next.getKey());
                    processMap.get(next.getKey()).setAckOffset(next.getValue());
                } catch (Throwable e) {
                    LOG.error("Failed to update offset to broker while resetting offset");
                }
            }

            if (LOG.isInfoEnabled()) {
                LOG.info("ResetOffset as follows");
                for (Map.Entry<MessageQueue, Long> next : offsetTable.entrySet()) {
                    LOG.info("Queue: {}, New offset: {}", next.getKey().toString(), next.getValue());
                }
            }
        }
    }

}
