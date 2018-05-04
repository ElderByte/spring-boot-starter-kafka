package com.elderbyte.kafka.tests;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpringBootTestApp.class)
public class SpringStarterKafkaTemplateTests {

	@Autowired
	private KafkaTemplate<String, Object> template;

	@Test
	public void templatesAvailable() {
		Assert.assertFalse(template.isTransactional());
	}

}
