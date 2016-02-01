package com.ndpmedia.flume.source.rocketmq;

/**
 * RocketMQSourceCounterMBean Created with rocketmq-flume.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 16/2/1 下午4:41
 * @desc
 */
public interface RocketMQSourceCounterMBean {

    long getEventReceivedTimer();

    long getEventAcceptedTimer();

}
