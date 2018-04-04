 [ ![Download](https://api.bintray.com/packages/elderbyte/maven/spring-boot-starter-kafka/images/download.svg) ](https://bintray.com/elderbyte/maven/spring-boot-starter-kafka/_latestVersion)

# spring-boot-starter-kafka
Spring Boot auto configuration for the spring Kafka client.


## Configuration

Configure the kafka client with **application.yml**

```yaml
kafka.client:
  enabled: true
  servers: localhost:9092
```

## Generic Json Serialisation

By default, this starter is very opiniated about serialisation:

* Message key is assumed to be of type `String`
* Message value is assumed to be of type `Json`

The `Json` object will decode the json byte array from the message into a generic json node. To decode this into your Java POJO just use the `.json(MyDto.class)` helper method.

The Json Encoder/Decoder use the Spring `ObjectMapper` bean.
