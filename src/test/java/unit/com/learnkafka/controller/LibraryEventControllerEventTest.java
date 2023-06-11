package com.learnkafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics="library-events", partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventControllerEventTest {

	@Autowired
	TestRestTemplate restTemplate;
	
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker; 
	
	Consumer<Integer, String> consumer;
	
	@BeforeEach
	void setup() {
		Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group", "true", embeddedKafkaBroker));
		consumer = new DefaultKafkaConsumerFactory(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
	}
	
//	@AfterEach
//	void tearDown() {
//		consumer.close();
//	}
	
	@Test
	void postLibraryEvent() throws InterruptedException {
		
		Book book = new Book();
		book.setBookAuthor("Suman");
		book.setBookId(1);
		book.setBookName("Kafka Development");
		
		LibraryEvent event = new LibraryEvent();
		event.setBook(book);
		event.setLibraryEventId(1);
		event.setLibraryEventType(LibraryEventType.NEW);
		
		HttpHeaders header = new HttpHeaders();
		header.add("Content-Type", MediaType.APPLICATION_JSON_VALUE.toString());
		
		HttpEntity<LibraryEvent> entity = new HttpEntity<>(event,header);
		
		ResponseEntity<LibraryEvent> response = restTemplate.exchange("/v1/libraryEvent", HttpMethod.POST , entity, LibraryEvent.class);
		
		assertEquals(HttpStatus.CREATED, response.getStatusCode());
		
		ConsumerRecord<Integer,String> consumerRecord =  KafkaTestUtils.getSingleRecord(consumer, "library-events");
		
		Thread.sleep(5000);
		
		String value = consumerRecord.value();
		String expectedRecord  = "{\"libraryEventId\": 1,\"libraryEventType\": \"NEW\",\"book\": {\"bookId\": 1,\"bookName\": \"Kafka Development\",\"bookAuthor\": \"Suman\"}}";
		
		assertEquals(value, expectedRecord);
		
	}
}
