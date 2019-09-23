package com.elderbyte.kafka.streams.builder.dsl;


import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ElMat { // TODO Rename? Store?

    /***************************************************************************
     *                                                                         *
     * Static Builder                                                          *
     *                                                                         *
     **************************************************************************/

    /**
     * Do not keep a local cache and also no backing log topic.
     */
    public static ElMat ephemeral(){
        return new ElMat(true)
                .loggingEnabled(false)
                .cachingEnabled(false);
    }

    /**
     * Keep only a local cache but no backing topic for restore.
     * If the local state is lost, all events must be reprocessed.
     */
    public static ElMat cached(String storeName){
        return cached()
                .storeName(storeName);
    }

    /**
     * Keep only a local cache but no backing topic for restore.
     * If the local state is lost, all events must be reprocessed.
     *
     * The cache store name is auto generated.
     */
    public static ElMat cached(){
        return new ElMat(false)
                .loggingEnabled(false);
    }

    /**
     * Keep a local cache and also create a backing log topic to restore state.
     */
    public static ElMat store(String storeName){
        return new ElMat(false).storeName(storeName);
    }

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private boolean ephemeral;

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
     * @param ephemeral
     */
    private ElMat(boolean ephemeral) {
        this.ephemeral = ephemeral;
    }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    public Optional<String> getStoreName() {
        return Optional.ofNullable(storeName);
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

    public boolean isEphemeral() {
        return ephemeral;
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
