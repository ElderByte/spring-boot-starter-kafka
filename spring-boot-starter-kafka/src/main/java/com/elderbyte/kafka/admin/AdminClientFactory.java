package com.elderbyte.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.KafkaException;

import java.util.Map;

/**
 * Provides the ability to create an kafka admin client.
 */
public class AdminClientFactory {

    private final Map<String, Object> config;

    public AdminClientFactory(Map<String, Object> config){
        this.config = config;
    }

    /**
     * Create a new kafka admin client and open connection.
     * @throws KafkaException Thrown when the admin client could not be created / connection failed.
     */
    public AdminClient create() throws KafkaException {
        return AdminClient.create(config);
    }
}
