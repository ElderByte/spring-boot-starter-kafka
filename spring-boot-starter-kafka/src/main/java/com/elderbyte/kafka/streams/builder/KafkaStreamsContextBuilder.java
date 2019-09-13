package com.elderbyte.kafka.streams.builder;

import com.elderbyte.kafka.streams.managed.KafkaStreamsContext;
import org.apache.kafka.streams.StreamsBuilder;


public interface KafkaStreamsContextBuilder {

    /***************************************************************************
     *                                                                         *
     * Builder API                                                             *
     *                                                                         *
     **************************************************************************/

    StreamsBuilder streamsBuilder();

    /***************************************************************************
     *                                                                         *
     * Builder Finalize                                                        *
     *                                                                         *
     **************************************************************************/

    KafkaStreamsContext build();

    /***************************************************************************
     *                                                                         *
     * Support API                                                             *
     *                                                                         *
     **************************************************************************/




}
