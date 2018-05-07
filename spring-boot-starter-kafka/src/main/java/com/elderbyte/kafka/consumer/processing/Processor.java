package com.elderbyte.kafka.consumer.processing;

@FunctionalInterface
public interface Processor<T> {
    void proccess(T data) throws Exception;
}
