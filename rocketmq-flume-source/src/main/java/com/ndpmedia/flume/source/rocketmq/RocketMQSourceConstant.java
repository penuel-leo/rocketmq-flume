package com.ndpmedia.flume.source.rocketmq;

/**
 * RocketMQSinkConstant Created with rocketmq-flume.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/9/17 上午10:42
 * @desc
 */
public class RocketMQSourceConstant {
    public static final String PROPERTY_PREFIX = "rocketmq.";


    /* Properties */
    public static final String TOPIC = "topic";
    public static final String CONSUMER_GROUP = "consumerGroup";
    public static final String TAG = "tag";
    public static final String ASYN = "asyn";
    public static final String MAX_SIZE = "maxSize";
    public static final String MAX_DELAY = "maxDelay";
    public static final String NAMESRVADDR = "namesrvAddr";
    public static final String MESSAGE_MODEL = "messageModel";
    public static final String CONSUME_FROM_WHERE = "consumeFromWhere";
    public static final String CONSUME_TIMESTAMP = "consumeTimestamp";
    public static final String EXTRA = "extra";
    public static final String PULL_BATCH_SIZE = "pullBatchSize";
    public static final String CONSUME_BATCH_SIZE = "consumeBatchSize";
    public static final String CORE_POOL_SIZE = "corePoolSize";
    public static final String MAX_POOL_SIZE = "maxPoolSize";


    /* default */
    public static final String DEFAULT_TOPIC = "T_ROCKETMQ_FLUME";
    public static final String DEFAULT_CONSUMER_GROUP = "CG_ROCKETMQ_FLUME";
    public static final String DEFAULT_TAG = "*";
    public static final String DEFAULT_MESSAGE_MODEL = "CLUSTERING";
    public static final String DEFAULT_CONSUME_FROM_WHERE = "CONSUME_FROM_LAST_OFFSET";
    public static final int DEFAULT_PULL_BATCH_SIZE = 128;
    public static final int DEFAULT_CONSUME_BATCH_SIZE = 8;
    public static final int DEFAULT_CORE_POOL_SIZE = 2;
    public static final int DEFAULT_MAX_POOL_SIZE = 4;

}
