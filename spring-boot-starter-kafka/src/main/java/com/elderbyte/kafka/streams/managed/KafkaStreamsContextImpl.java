package com.elderbyte.kafka.streams.managed;

import com.elderbyte.commons.exceptions.ArgumentNullException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.KafkaStreamsCustomizer;
import org.springframework.kafka.core.CleanupConfig;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Represents a KafkaStreams Topology
 */
public class KafkaStreamsContextImpl implements KafkaStreamsContext  {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsContextImpl.class);

    private final KafkaStreamsConfiguration streamsConfig;
    private final StreamsCleanupConfig cleanupConfig;
    private final Topology topology;


    private KafkaStreams kafkaStreams = null;

    private KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();

    private KafkaStreamsCustomizer kafkaStreamsCustomizer;

    private KafkaStreams.StateListener stateListener;
    private StateRestoreListener stateRestoreListener;
    private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    private boolean autoStartup = true;
    private int phase = Integer.MAX_VALUE - 1000; // NOSONAR magic #
    private int closeTimeout = 10;
    private volatile boolean running;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/

    /**
     * Creates a new KafkaStreamsBuilderImpl
     */
    public KafkaStreamsContextImpl(
            Topology topology,
            KafkaStreamsConfiguration streamsConfig,
            StreamsCleanupConfig cleanupConfig,
            KafkaStreamsCustomizer kafkaStreamsCustomizer
    ) {
        if(topology == null) throw new ArgumentNullException("topology");
        if(streamsConfig == null) throw new ArgumentNullException("streamsConfig");
        if(cleanupConfig == null) throw new ArgumentNullException("cleanupConfig");

        this.topology = topology;
        this.streamsConfig = streamsConfig;
        this.cleanupConfig = cleanupConfig;
        this.kafkaStreamsCustomizer = kafkaStreamsCustomizer;


        uncaughtExceptionHandler = (t, ex) -> {
            logger.error("Kafka Streams thread "+t.getName()+" died due unhandled exception!", ex);
        };


        stateRestoreListener = new StateRestoreListener() {
            @Override
            public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
                logger.info("Start Restoring Store: " + storeName + " at topic " + topicPartition + " for " + startingOffset + " - " + endingOffset + "...");
            }

            @Override
            public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
                logger.info("Restoring Store: " + storeName + " at topic " + topicPartition + " restored: " + numRestored);
            }

            @Override
            public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
                logger.info("Completed Restored Store : " + storeName + " at topic " + topicPartition + " total restored: " + totalRestored);
            }
        };

        stateListener = (state, old) -> {
            logger.info("State: " + state);
            if(state == KafkaStreams.State.ERROR){
                if(cleanupConfig.cleanupOnError()){
                    logger.warn("Since Kafka Streams died in an Error, we clean up local storage to improve recovery chances!");
                    kafkaStreams.cleanUp();
                }
            }
        };
    }

    /***************************************************************************
     *                                                                         *
     * Properties                                                              *
     *                                                                         *
     **************************************************************************/

    public Topology getTopology(){
        return topology;
    }

    public Optional<KafkaStreams> getKafkaStreams(){
        return Optional.ofNullable(kafkaStreams);
    }


    /***************************************************************************
     *                                                                         *
     * Smart Lifecycle                                                         *
     *                                                                         *
     **************************************************************************/

    public void setAutoStartup(boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    public void setPhase(int phase) {
        this.phase = phase;
    }

    @Override
    public boolean isAutoStartup() {
        return this.autoStartup;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        if (callback != null) {
            callback.run();
        }
    }

    @Override
    public synchronized void start() {
        if (!this.running) {
            try {
                var properties = streamsConfig.asProperties();
                this.kafkaStreams = new KafkaStreams(topology, properties, this.clientSupplier);
                this.kafkaStreams.setStateListener(this.stateListener);
                this.kafkaStreams.setGlobalStateRestoreListener(this.stateRestoreListener);
                this.kafkaStreams.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);
                if (this.kafkaStreamsCustomizer != null) {
                    this.kafkaStreamsCustomizer.customize(this.kafkaStreams);
                }
                if (this.cleanupConfig.cleanupOnStart()) {
                    this.kafkaStreams.cleanUp();
                }
                this.kafkaStreams.start();
                this.running = true;
            }
            catch (Exception e) {
                throw new KafkaException("Could not start stream: ", e);
            }
        }
    }

    @Override
    public synchronized void stop() {
        if (this.running) {
            try {
                if (this.kafkaStreams != null) {
                    this.kafkaStreams.close(this.closeTimeout, TimeUnit.SECONDS);
                    if (this.cleanupConfig.cleanupOnStop()) {
                        this.kafkaStreams.cleanUp();
                    }
                    this.kafkaStreams = null;
                }
            }
            catch (Exception e) {
                logger.error("Failed to stop streams", e);
            }
            finally {
                this.running = false;
            }
        }
    }

    @Override
    public synchronized boolean isRunning() {
        return this.running;
    }


    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
