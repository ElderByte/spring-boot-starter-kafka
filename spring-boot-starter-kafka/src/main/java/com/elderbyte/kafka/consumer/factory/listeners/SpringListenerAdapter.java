package com.elderbyte.kafka.consumer.factory.listeners;

import com.elderbyte.kafka.consumer.factory.KafkaListenerConfiguration;
import com.elderbyte.kafka.consumer.factory.processor.ManagedProcessor;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.GenericMessageListener;
import org.springframework.kafka.listener.MessageListener;

public class SpringListenerAdapter {

    public static  <K,V> GenericMessageListener<?> buildListenerAdapter(KafkaListenerConfiguration<K,V> config, ManagedProcessor<K,V> managedProcessor){

        boolean batch = config.isBatch();

        if(batch){
            return buildBatchListener(managedProcessor, config.isManualAck());
        }else{
            return buildListener(managedProcessor, config.isManualAck());
        }
    }

    private static  <K,V> BatchMessageListener<byte[], byte[]> buildBatchListener(ManagedProcessor<K,V> managedProcessor, boolean manualAck){
        if(manualAck){
            return new ManagedAckBatchRawListener<>(managedProcessor);
        }else{
            return new ManagedBatchRawListener<>(managedProcessor);
        }
    }

    private static  <K,V> MessageListener<byte[], byte[]> buildListener(ManagedProcessor<K,V> managedProcessor, boolean manualAck){
        if(manualAck){
            return new ManagedAckRawListener<>(managedProcessor);
        }else{
            return new ManagedRawListener<>(managedProcessor);
        }
    }

}
