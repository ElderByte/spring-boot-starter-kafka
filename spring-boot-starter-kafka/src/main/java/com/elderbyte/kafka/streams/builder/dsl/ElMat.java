package com.elderbyte.kafka.streams.builder.dsl;


import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class ElMat {

    public static ElMat store(String storeName){
        return new ElMat().storeName(storeName);
    }

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    // Local state store name and part of the generated log topic
    private String storeName;

    // Local state store
    private boolean cachingEnabled = true;

    // Backing Kafka topic
    private boolean loggingEnabled = true;
    private Map<String, String> topicConfig = new HashMap<>();
    private Duration retention;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new ElMat
     */
    private ElMat() { }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    public String getStoreName() {
        return storeName;
    }

    public ElMat storeName(String storeName) {
        this.storeName = storeName;
        return this;
    }

    public boolean isLoggingEnabled() {
        return loggingEnabled;
    }

    public ElMat loggingEnabled(boolean loggingEnabled) {
        this.loggingEnabled = loggingEnabled;
        return this;
    }

    public boolean isCachingEnabled() {
        return cachingEnabled;
    }

    public ElMat cachingEnabled(boolean cachingEnabled) {
        this.cachingEnabled = cachingEnabled;
        return this;
    }

    public Duration getRetention() {
        return retention;
    }

    public Map<String, String> getTopicConfig() {
        return topicConfig;
    }

    public ElMat topicConfig(Map<String, String> topicConfig) {
        this.topicConfig = topicConfig;
        return this;
    }

    public ElMat retention(Duration retention) {
        this.retention = retention;
        return this;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
