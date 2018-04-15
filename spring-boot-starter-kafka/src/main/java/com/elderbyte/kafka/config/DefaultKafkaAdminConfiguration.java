package com.elderbyte.kafka.config;

import com.elderbyte.kafka.admin.AdminClientFactory;
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

    @Autowired
    private KafkaClientConfig config;


    @Bean
    public KafkaAdmin admin() {
        return new KafkaAdmin(adminConfig());
    }

    @Bean
    public AdminClientFactory adminClientFactory(){
        return new AdminClientFactory(adminConfig());
    }

    private Map<String, Object> adminConfig(){
        var configs = new HashMap<String, Object>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaServers());
        return configs;
    }

}
