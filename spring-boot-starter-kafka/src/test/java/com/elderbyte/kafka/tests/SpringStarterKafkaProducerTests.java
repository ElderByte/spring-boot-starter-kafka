package com.elderbyte.kafka.tests;

import com.elderbyte.kafka.producer.KafkaProducer;
import com.elderbyte.kafka.producer.KafkaProducerTx;
import com.elderbyte.kafka.producer.impl.KafkaProducerImpl;
import com.elderbyte.kafka.producer.impl.KafkaProducerTxImpl;
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
        "kafka.client.producer.transaction.id=SpringStarterKafkaProducerTests"
})
public class SpringStarterKafkaProducerTests {

	@Autowired
	private KafkaProducer<String, Object> producer;

	@Autowired
	private KafkaProducerTx<String, Object> producerTx;

	@Test
	public void producersAreWorking() {
		Assert.assertTrue(producer instanceof KafkaProducerImpl);
		Assert.assertFalse(producer instanceof KafkaProducerTxImpl);
		Assert.assertTrue(producerTx instanceof KafkaProducerTxImpl);
	}
}
