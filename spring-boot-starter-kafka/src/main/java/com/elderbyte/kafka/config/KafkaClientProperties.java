package com.elderbyte.kafka.config;

import com.elderbyte.kafka.topics.TopicProperties;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties("kafka.client")
public class KafkaClientProperties {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private boolean enabled = true;
    private String servers;

    private ConsumerProperties consumer = new ConsumerProperties();
    private ProducerProperties producer = new ProducerProperties();
    private StreamsProperties streams = new StreamsProperties();
    private List<TopicProperties> topics = new ArrayList<>();

    /***************************************************************************
     *                                                                         *
     *  Properties                                                             *
     *                                                                         *
     **************************************************************************/

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public ConsumerProperties getConsumer() {
        return consumer;
    }

    public void setConsumer(ConsumerProperties consumer) {
        this.consumer = consumer;
    }

    public ProducerProperties getProducer() {
        return producer;
    }

    public void setProducer(ProducerProperties producer) {
        this.producer = producer;
    }

    public List<TopicProperties> getTopics() {
        return topics;
    }

    public void setTopics(List<TopicProperties> topics) {
        this.topics = topics;
    }

    public StreamsProperties getStreams() {
        return streams;
    }

    public void setStreams(StreamsProperties streams) {
        this.streams = streams;
    }

    /***************************************************************************
     *                                                                         *
     *  Inner Classes                                                          *
     *                                                                         *
     **************************************************************************/

    public static class StreamsProperties {

        private Duration commitInterval = Duration.ofSeconds(10);
        private String processingGuarantee = StreamsConfig.AT_LEAST_ONCE; // exactly_once
        private int replicationFactor = 1; // Set to 3 for HA
        private String producerAcks = "1"; // all for HA
        private String stateDir = null;
        private String optimizeTopology = StreamsConfig.OPTIMIZE; // default none

        public Duration getCommitInterval() {
            return commitInterval;
        }

        public void setCommitInterval(Duration commitInterval) {
            this.commitInterval = commitInterval;
        }

        public String getProcessingGuarantee() {
            return processingGuarantee;
        }

        public void setProcessingGuarantee(String processingGuarantee) {
            this.processingGuarantee = processingGuarantee;
        }

        public int getReplicationFactor() {
            return replicationFactor;
        }

        public void setReplicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
        }

        public String getProducerAcks() {
            return producerAcks;
        }

        public void setProducerAcks(String producerAcks) {
            this.producerAcks = producerAcks;
        }

        public String getStateDir() {
            return stateDir;
        }

        public void setStateDir(String stateDir) {
            this.stateDir = stateDir;
        }

        public String getOptimizeTopology() {
            return optimizeTopology;
        }

        public void setOptimizeTopology(String optimizeTopology) {
            this.optimizeTopology = optimizeTopology;
        }
    }

    public static class ProducerProperties {

        private Transaction transaction = new Transaction();

        public Transaction getTransaction() {
            return transaction;
        }

        public void setTransaction(Transaction transaction) {
            this.transaction = transaction;
        }


        public static class Transaction {
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }
    }

    public static class ConsumerProperties {
        private Integer concurrency = null;
        private Integer pollTimeout = null;
        private Integer maxPollRecords = null;
        private boolean autoCommit = true;
        private String autoOffsetReset = "earliest";

        public Integer getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(Integer concurrency) {
            this.concurrency = concurrency;
        }

        public Integer getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(Integer pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        public Integer getMaxPollRecords() {
            return maxPollRecords;
        }

        public void setMaxPollRecords(Integer maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
        }

        public boolean isAutoCommit() {
            return autoCommit;
        }

        public void setAutoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
        }

        public String getAutoOffsetReset() {
            return autoOffsetReset;
        }

        public void setAutoOffsetReset(String autoOffsetReset) {
            this.autoOffsetReset = autoOffsetReset;
        }
    }
}
