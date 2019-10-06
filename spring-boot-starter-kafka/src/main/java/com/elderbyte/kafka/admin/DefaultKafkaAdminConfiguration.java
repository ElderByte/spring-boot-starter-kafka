package com.elderbyte.kafka.admin;

import com.elderbyte.kafka.config.KafkaClientProperties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
@ConditionalOnProperty(value = "kafka.client.admin.enabled", matchIfMissing = true)
public class DefaultKafkaAdminConfiguration {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    @Autowired
    private KafkaClientProperties config;

    @Value("${spring.application.name:}")
    private String applicationName;

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
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getServers());
        configs.put(AdminClientConfig.CLIENT_ID_CONFIG, defaultAdminClientId());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return configs;
    }

    private String defaultAdminClientId(){
        String appName;
        if(applicationName != null && !applicationName.isBlank()){
            appName = applicationName;
        }else{
            var uuidParts = UUID.randomUUID().toString().split("-");
            appName = "spring-" + uuidParts[uuidParts.length-1];
        }
        return appName + "-admin";
    }

}
