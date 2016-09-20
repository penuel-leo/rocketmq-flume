package com.ndpmedia.flume.source.rocketmq;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ProcessQueue {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessQueue.class);

    private static final long INTERVAL_10MIN_IN_MS = 10 * 60 * 1000L;

    private static final long FLOW_CONTROL_ACCUMULATION_THRESHOLD = 10000;

    private static final long FLOW_CONTROL_CONSUMING_SPAN_THRESHOLD = 5000;

    private long lastPullTimestamp = System.currentTimeMillis();
    private boolean dropped;
    private final MessageQueue messageQueue;
    private final TreeMap<Long, MessageExt> treeMap;
    private final TreeSet<Long> window;
    private final ReadWriteLock lock;
    private volatile long maxOffset;
    private volatile long ackOffset;

    private volatile boolean consumeOffsetPersisted;


    public ProcessQueue(final MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
        treeMap = new TreeMap<>();
        window = new TreeSet<>();
        lock = new ReentrantReadWriteLock(false);
        consumeOffsetPersisted = true;
    }

    public void putMessages(List<MessageExt> messageList) {
        if (null == messageList || messageList.isEmpty()) {
            return;
        }

        lock.writeLock().lock();
        try {
            for (MessageExt message : messageList) {
                treeMap.put(message.getQueueOffset(), message);
                if (message.getQueueOffset() > maxOffset) {
                    maxOffset = message.getQueueOffset();
                }
            }
        } finally {
            lock.writeLock().unlock();
            LOGGER.debug("Pulled {} messages", messageList.size());
        }
    }

    public void ack(List<MessageExt> messageList) {
        if (null == messageList || messageList.isEmpty()) {
            return;
        }

        lock.writeLock().lock();
        try {
            for (MessageExt message : messageList) {
                treeMap.remove(message.getQueueOffset());
                window.add(message.getQueueOffset());
            }

            if (ackOffset <= 0) {
                ackOffset = window.first() - 1;
            }

            while (true) {
                if (window.isEmpty()) {
                    break;
                }

                if (window.first() == ackOffset + 1) {
                    ackOffset++;
                    window.pollFirst();
                    consumeOffsetPersisted = false;
                } else if (window.first() <= ackOffset) {
                    window.pollFirst();
                } else {
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
            LOGGER.debug("Acknowledged {} messages. Accumulation: {}, Flow Control: {}, Ack: {}",
                    messageList.size(),
                    treeMap.size(),
                    needFlowControl(),
                    ackOffset);
        }
    }

    public List<MessageExt> peek(int n) {
        List<MessageExt> result = new ArrayList<>();
        lock.readLock().lock();
        try {
            int count = 0;
            for (Map.Entry<Long, MessageExt> next : treeMap.entrySet()) {
                result.add(next.getValue());
                count++;

                if (count >= n) {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        return result;
    }

    public boolean hasPendingMessage() {
        return !treeMap.isEmpty();
    }

    public boolean needFlowControl() {
        return treeMap.size() >= FLOW_CONTROL_ACCUMULATION_THRESHOLD
                || consumingWindowSpan() >= FLOW_CONTROL_CONSUMING_SPAN_THRESHOLD;
    }

    public long consumingWindowSpan() {
        if (window.isEmpty()) {
            return 0;
        }

        lock.readLock().lock();
        try {
            if (window.isEmpty()) {
                return 0;
            } else {
                return window.last() - window.first();
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public void refreshLastPullTimestamp() {
        lastPullTimestamp = System.currentTimeMillis();
    }

    public boolean isPullAlive() {
        return System.currentTimeMillis() - lastPullTimestamp < INTERVAL_10MIN_IN_MS;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public long getAckOffset() {
        return ackOffset;
    }

    public void setAckOffset(long ackOffset) {
        lock.writeLock().lock();
        try {
            this.ackOffset = ackOffset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean isConsumeOffsetPersisted() {
        return consumeOffsetPersisted;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }
}
