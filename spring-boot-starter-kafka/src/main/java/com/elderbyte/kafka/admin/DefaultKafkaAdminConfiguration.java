package com.elderbyte.kafka.admin;

import com.elderbyte.kafka.config.KafkaClientConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnProperty(value = "kafka.client.admin.enabled", matchIfMissing = true)
public class DefaultKafkaAdminConfiguration {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    @Autowired
    private KafkaClientConfig config;

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(adminConfig());
    }

    @Bean
    public AdminClientFactory adminClientFactory(){
        return new AdminClientFactory(adminConfig());
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private Map<String, Object> adminConfig(){
        var configs = new HashMap<String, Object>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaServers());
        return configs;
    }

}
