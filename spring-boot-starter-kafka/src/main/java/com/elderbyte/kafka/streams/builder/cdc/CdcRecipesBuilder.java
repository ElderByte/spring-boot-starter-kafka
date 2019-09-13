package com.elderbyte.kafka.streams.builder.cdc;

import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.HashSet;
import java.util.Set;

public class CdcRecipesBuilder {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KafkaStreamsContextBuilder builder;

    /***************************************************************************
     *                                                                         *
     * Constructor                                                             *
     *                                                                         *
     **************************************************************************/


    public CdcRecipesBuilder(KafkaStreamsContextBuilder builder){
        this.builder = builder;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/


    public <CDCEvent, Entity> KTable<String, Entity> cdcStreamAsTable(
            String storeName,
            KStream<String, CDCEvent> cdcEventStream,
            KeyValueMapper<String, CDCEvent, KeyValue<String,Entity>> kvm,
            Class<Entity> clazz
    ){
        return cdcEventStream
        .map(kvm::apply)
        .groupByKey(builder.serializedJson(clazz))
        .reduce(
                (old, current) -> current, // TODO Handle deletes from current.deleted flag
                builder.materializedJson(storeName, clazz)
                        .withLoggingDisabled() // Good bad ?
        );
    }

    public <InK,InV, OutV> KTable<String, Set<OutV>> aggregateSet(
            String storeName,
            KTable<InK, InV> compactedTable,
            KeyValueMapper<InK, InV, KeyValue<String,OutV>> kvm,
            Class<OutV> clazz,
            TypeReference<Set<OutV>> setClazz
    ){
        return compactedTable.groupBy(
                kvm::apply,
                builder.serializedJson(clazz)
        )
                .aggregate(
                        HashSet::new,
                        (k,v, agg) -> { agg.add(v); return agg; }, // Adder
                        (k,v, agg) -> { agg.remove(v); return agg; }, // Remover
                        builder.materializedJson(storeName, setClazz)
                );
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

}
