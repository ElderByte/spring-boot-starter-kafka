package com.elderbyte.kafka.streams.builder.cdc;

import com.elderbyte.kafka.streams.builder.KafkaStreamsContextBuilder;
import com.elderbyte.kafka.streams.builder.TombstoneJsonWrapper;
import com.elderbyte.kafka.streams.serdes.ElderKeySerde;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
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


    public <InK,InV, OutK, OutV> KTable<OutK, Set<OutV>> aggregateSet(
            String storeName,
            KTable<InK, InV> compactedTable,
            KeyValueMapper<InK, InV, KeyValue<OutK,OutV>> kvm,
            Class<OutK> keyClazz,
            Class<OutV> valueClazz,
            TypeReference<Set<OutV>> setClazz
    ){
        var keySerde = ElderKeySerde.from(keyClazz);


        return compactedTable.groupBy(
                kvm,
                builder.groupedJson(keySerde, valueClazz)
        )
                .aggregate(
                        HashSet::new,
                        (k,v, agg) -> { agg.add(v); return agg; }, // Adder
                        (k,v, agg) -> { agg.remove(v); return agg; }, // Remover
                        builder.materializedJson(storeName, keySerde, setClazz)
                );
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    /*
    //private final ObjectMapper mapper;
    private <V> Set<V> test(Class<V> clazz){
        var colType = mapper.getTypeFactory().constructCollectionType(Set.class, clazz);
        try {
            return  mapper.readValue("Sadf", colType);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }*/

}
