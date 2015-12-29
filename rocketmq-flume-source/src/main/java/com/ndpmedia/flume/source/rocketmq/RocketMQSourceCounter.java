package com.ndpmedia.flume.source.rocketmq;

import org.apache.flume.instrumentation.SourceCounter;

/**
 * RocketMQSourceCounter Created with rocketmq-flume.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/12/29 下午4:47
 * @desc
 */
public class RocketMQSourceCounter extends SourceCounter{

    private static final String TIMER_RMQ_EVENT_RECEIVED =
            "source.rmq.event.received.time";

    private static final String TIMER_RMQ_EVENT_ACCEPTED =
            "source.rmq.event.accepted.time";

    private static final String[] ATTRIBUTES =
            {TIMER_RMQ_EVENT_RECEIVED, TIMER_RMQ_EVENT_ACCEPTED};

    public RocketMQSourceCounter(String name) {
        super(name, ATTRIBUTES);
    }

    public RocketMQSourceCounter(String name, String[] attributes) {
        super(name, attributes);
    }

    public long addToEventReceivedTimer(long delta) {
        return addAndGet(TIMER_RMQ_EVENT_RECEIVED,delta);
    }
    public long addToEventAcceptedTimer(long delta) {
        return addAndGet(TIMER_RMQ_EVENT_ACCEPTED,delta);
    }
}
