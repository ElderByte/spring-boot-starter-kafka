package com.elderbyte.kafka.consumer;

@FunctionalInterface
public interface Processor<T> {
    void proccess(T data) throws Exception;
}
