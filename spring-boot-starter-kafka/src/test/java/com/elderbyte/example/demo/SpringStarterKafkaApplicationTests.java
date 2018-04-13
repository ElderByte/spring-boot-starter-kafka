package com.elderbyte.example.demo;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringStarterKafkaApplicationTests {

	@Autowired
	private KafkaTemplate<String, Object> template;

	@Autowired
	@Qualifier("kafkaTemplateTransactional")
	private KafkaTemplate<String, Object> txtemplate;

	@Test
	public void contextLoads() {
	}

	@Test
	public void templatesAvailable() {
		Assert.assertNotEquals(template, txtemplate);

		Assert.assertTrue(txtemplate.isTransactional());
		Assert.assertFalse(template.isTransactional());
	}

}
