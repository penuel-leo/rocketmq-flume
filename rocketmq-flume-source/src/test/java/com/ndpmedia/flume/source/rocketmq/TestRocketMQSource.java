package com.ndpmedia.flume.source.rocketmq;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestRocketMQSource {

    @BeforeClass
    public static void setUp(){
        System.setProperty("enable_ssl", "true");
        System.setProperty("rocketmq.namesrv.domain", "172.30.30.125");
        System.setProperty("log.home", "/var/tmp");
    }

    @Test
    public void testPull() throws EventDeliveryException {
        RocketMQSource rocketMQSource = new RocketMQSource();
        Map<String, String> config = new HashMap<String, String>();
        config.put("topic", "T_QuickStart");
        config.put("consumerGroup", "CG_QuickStart");
        config.put("tag", "*");
        Context context = new Context(config);
        rocketMQSource.configure(context);
        rocketMQSource.start();
        rocketMQSource.process();

    }

}
