package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
//@EnableKafka
public class AutoCreateConfig {

	@Bean
	public NewTopic libraryEvents() {
		return TopicBuilder.name("library-events-1")
		.partitions(3)
		.replicas(3)
		.build();
	}
	
}
