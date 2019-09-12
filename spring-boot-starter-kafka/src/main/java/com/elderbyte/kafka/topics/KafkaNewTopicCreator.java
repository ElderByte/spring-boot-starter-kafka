package com.elderbyte.kafka.topics;

import com.elderbyte.commons.utils.Stopwatch;
import com.elderbyte.kafka.admin.AdminClientFactory;
import com.elderbyte.kafka.config.KafkaClientProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Holds all Kafka Topic configurations.
 */
public class KafkaNewTopicCreator {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private static final Logger log = LoggerFactory.getLogger(KafkaNewTopicCreator.class);

    private final AdminClientFactory adminClientFactory;
    private final KafkaClientProperties kafkaConfig;

    /***************************************************************************
     *                                                                         *
     * Life Cycle                                                              *
     *                                                                         *
     **************************************************************************/

    public KafkaNewTopicCreator(
            KafkaClientProperties kafkaConfig,
            AdminClientFactory adminClientFactory
    ){
        this.kafkaConfig = kafkaConfig;
        this.adminClientFactory = adminClientFactory;
    }

    @PostConstruct
    public void init() {
        try{
            createTopics(kafkaConfig.getTopics());
        }catch (Exception e){
            log.error("Failed to initialize KafkaAdmin", e);
        }
    }

    public void createTopics(Collection<TopicProperties> topicProperties){
        try (final AdminClient kafkaClient = adminClientFactory.create()) {

            log.debug("Creating topics: " + topicProperties.stream().map(TopicProperties::toString).collect(joining(",")));

            var watch = Stopwatch.started();
            var newTopics = newTopics(topicProperties);
            kafkaClient.createTopics(newTopics);

            log.info("Created " + newTopics.size() + " topics in " + watch.sinceStartMs() + "ms!");

        } catch (Exception e) {
            throw new IllegalStateException("Failed to create initial topics!", e);
        }
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    private List<NewTopic> newTopics(Collection<TopicProperties> topicProperties){
        return topicProperties.stream()
                    .filter(TopicProperties::isCreate)
                    .map(this::newTopic)
                    .collect(toList());
    }

    private NewTopic newTopic(TopicProperties topicProperties){
        var newTopic = new NewTopic(
                topicProperties.getName(),
                topicProperties.getPartitions(),
                topicProperties.getReplicas()
        );
        var topicConfig = new HashMap<String, String>();
        topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, topicProperties.getCleanUpPolicy());
        if(topicProperties.getRetention() != null){
            topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, topicProperties.getRetention().toMillis() + "");
        }
        topicConfig.putAll(topicProperties.getProperties());
        newTopic.configs(topicConfig);

        return newTopic;
    }


}
