package com.elderbyte.kafka.consumer.factory;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

public class MockMessageListenerContainer implements MessageListenerContainer {

    private Map<String, Map<MetricName, ? extends Metric>> metrics = new HashMap<>();

    @Override
    public void setupMessageListener(Object messageListener) {

    }

    @Override
    public Map<String, Map<MetricName, ? extends Metric>> metrics() {
        return metrics;
    }

    @Override
    public boolean isAutoStartup() {
        return false;
    }

    @Override
    public void stop(Runnable callback) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public int getPhase() {
        return 0;
    }
}
