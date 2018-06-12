 [ ![Download](https://api.bintray.com/packages/elderbyte/maven/spring-boot-starter-kafka/images/download.svg) ](https://bintray.com/elderbyte/maven/spring-boot-starter-kafka/_latestVersion)
[![CircleCI](https://circleci.com/gh/ElderByte-/spring-boot-starter-kafka.svg?style=svg)](https://circleci.com/gh/ElderByte-/spring-boot-starter-kafka)

# spring-boot-starter-kafka
Spring Boot auto configuration for the spring Kafka client.


## Configuration

Configure the kafka client with **application.yml**


minimal config

```yaml
kafka.client:
  servers: localhost:9092
```

advanced config

```yaml
kafka.client:
  servers: localhost:9092
  consumer:
    enabled: true
    autoCommit: false # Enables manual ACK
    concurrency: 3
    pollTimeout: 3000
    maxPollRecords: 10
  producer:
    transaction.id: my-producer-instance # Enables transaction support
```

Do disable the auto-configuration completely, set `kafka.client.enabled` to `false`
You can also disable specific features of this auto-configuration, if you don't need all of it:

```yaml
kafka.client:
    admin.enabled: false
    consumer.enabled: false
    producer.enabled: false
```



## Generic Json Serialisation

By default, this starter is very opiniated about serialisation:

* Message key is assumed to be of type `String`
* Message value is assumed to be of type `Json`

The `Json` object will decode the json byte array from the message into a generic json node. To decode this into your Java POJO just use the `.json(MyDto.class)` helper method.

The Json Encoder/Decoder use the Spring `ObjectMapper` bean.



## Advanced Kafka listener

* Fluent api for standard listener config
* All config of listener in one place
* Posion Message handling with retry logic


```java
@Service
public class MyListener {

  @Autowired
  public MyListener(KafkaListenerFactory factory){
    
    factory.start("my-topic")
 		.consumerGroup("group-xyz")
    	.autoCommit(false)

		.keyDeserializer() // Default to string
		.valueDeserializer() // Default to string
		
		.valueJson(MyDto.class)

    	// Retry config   
		.retry(2)   // retry locally 2 times
		.delay(300) // ms delay locally
		.asyncRetry(10) // -> creates a retry topic: my-topic.retry
		.asyncRetryDelay(60000)
		.deadLetter() // -> creates a dead-letter topic
        
        .listenBatch(records -> {
        	// Do someting with the records
        });   

  }

}
```

