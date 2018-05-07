package com.elderbyte.kafka.consumer.processing;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExceptionClassUtil {

    public static List<Class<? extends Exception>> asList(Class<? extends Exception>... exceptions){
        return Arrays.asList(exceptions);
    }


    public static Set<Class<? extends Exception>> asSet(Class<? extends Exception>... exceptions){
        return new HashSet<>(asList(exceptions));
    }
}
