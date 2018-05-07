package com.elderbyte.kafka.tests;

import com.elderbyte.kafka.consumer.processing.ManagedProcessorFactory;
import com.elderbyte.kafka.metrics.MetricsContext;
import com.elderbyte.kafka.producer.KafkaProducer;
import com.elderbyte.kafka.producer.mock.KafkaProducerMock;
import com.elderbyte.kafka.producer.mock.KafkaProducerTxMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringBootTestApp.class)
@TestPropertySource(properties = {
        "kafka.client.enabled=false"
})
public class SpringStarterKafkaManagedProcessorTests {

	@Autowired
	private ManagedProcessorFactory processorFactory;

	@Test
	public void producersAreMocked() {

		var managedProcessor = processorFactory.buildSkipping(Object.class, MetricsContext.from("unit-app", "instance-id"));

		Assert.assertNotNull(managedProcessor);
	}
}
