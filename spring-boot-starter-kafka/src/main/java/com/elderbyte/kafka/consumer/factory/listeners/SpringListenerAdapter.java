package com.elderbyte.kafka.consumer.factory.listeners;

import com.elderbyte.kafka.consumer.factory.KafkaListenerConfiguration;
import com.elderbyte.kafka.consumer.factory.processor.ManagedProcessor;

public class SpringListenerAdapter {

    public static  <K,V> Object buildListenerAdapter(KafkaListenerConfiguration<K,V> config, ManagedProcessor<K,V> managedProcessor){

        boolean batch = config.isBatch();
        boolean manualAck = config.isManualAck();

        if(batch){
            if(manualAck){
                return new ManagedAckBatchRawListener<>(managedProcessor);
            }else{
                return new ManagedBatchRawListener<>(managedProcessor);
            }
        }else{
            if(manualAck){
                return new ManagedAckRawListener<>(managedProcessor);
            }else{
                return new ManagedRawListener<>(managedProcessor);
            }
        }
    }

}
