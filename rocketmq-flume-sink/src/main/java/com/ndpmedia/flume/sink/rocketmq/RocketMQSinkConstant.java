package com.ndpmedia.flume.sink.rocketmq;

/**
 * RocketMQSinkConstant Created with rocketmq-flume.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/9/17 上午10:42
 * @desc
 */
public class RocketMQSinkConstant {
    public static final String PROPERTY_PREFIX = "rocketmq.";


    /* Properties */
    public static final String TOPIC = "topic";
    public static final String PRODUCER_GROUP = "producerGroup";
    public static final String TAG = "tag";
    public static final String ALLOW = "allow";
    public static final String DENY = "deny";
    public static final String ASYN = "asyn";
    public static final String NAMESRVADDR = "namesrvAddr";

    /* defalut */
    public static final String DEFAULT_TOPIC = "T_ROCKETMQ_FLUME";
    public static final String DEFAULT_PRODUCER_GROUP = "PG_ROCKETMQ_FLUME";
    public static final String DEFAULT_TAG = "";
}
