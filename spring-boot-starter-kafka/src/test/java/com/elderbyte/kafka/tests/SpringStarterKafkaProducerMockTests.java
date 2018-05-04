package com.elderbyte.kafka.tests;

import com.elderbyte.kafka.producer.KafkaProducer;
import com.elderbyte.kafka.producer.KafkaProducerTx;
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
public class SpringStarterKafkaProducerMockTests {

	@Autowired
	private KafkaProducer<String, Object> producer;

	@Test
	public void producersAreMocked() {
		Assert.assertTrue(producer instanceof KafkaProducerMock);
		Assert.assertFalse(producer instanceof KafkaProducerTxMock);
	}
}
