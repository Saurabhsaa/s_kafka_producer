package com.learnkafka.producer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class LibraryEventProducer {

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

		CompletableFuture<SendResult<Integer, String>> compFuture = kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),
				new ObjectMapper().writeValueAsString(libraryEvent));

		System.out.println("Success sent");
		
	}
	
	public SendResult<Integer, String> sendLibraryEventSync(LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException {

		SendResult<Integer, String> rest = kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),
				new ObjectMapper().writeValueAsString(libraryEvent)).get();
		System.out.println("Success sent");
		return rest;
	}
	
	public SendResult<Integer, String> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {

		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(libraryEvent.getLibraryEventId(),
				new ObjectMapper().writeValueAsString(libraryEvent), "library-events-1");

		SendResult<Integer, String> rest = kafkaTemplate.send(producerRecord).get(2,TimeUnit.SECONDS);
		System.out.println("Success sent");
		return rest;
	}

	private ProducerRecord<Integer, String> buildProducerRecord(Integer libraryEventId, String writeValueAsString, String topic) {
		
		List<Header> headers = List.of(new RecordHeader("event-source", "scanner".getBytes()));
		
		return new ProducerRecord<>(topic, null, libraryEventId, writeValueAsString, headers);
		
	}
	
	
}
