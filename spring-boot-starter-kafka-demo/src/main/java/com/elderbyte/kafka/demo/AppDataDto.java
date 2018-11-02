package com.elderbyte.kafka.demo;

public class AppDataDto {

    public String name;
    public int count;

    public AppDataDto(){}

    public AppDataDto(String name, int count) {
        this.name = name;
        this.count = count;
    }

    @Override
    public String toString() {
        return "AppDataDto{" +
                "name='" + name + '\'' +
                ", count=" + count +
                '}';
    }
}
