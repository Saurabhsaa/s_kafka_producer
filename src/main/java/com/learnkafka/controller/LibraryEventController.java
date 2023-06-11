package com.learnkafka.controller;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class LibraryEventController {
	
	@Autowired
	LibraryEventProducer eventProducer;

	@PostMapping(value="/v1/libraryEvent", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<?> postLibraryEvent(@RequestBody LibraryEvent event) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException{
		
		System.out.println("Before ...");
//		eventProducer.sendLibraryEvent(event);
//		SendResult<Integer, String> result =  eventProducer.sendLibraryEventSync(event);
		event.setLibraryEventType(LibraryEventType.NEW);
		SendResult<Integer, String> result =  eventProducer.sendLibraryEvent_Approach2(event);
		System.out.println("After ..."+result.toString());
		
		return ResponseEntity.status(HttpStatus.CREATED).body(event);
	}
	
	@PutMapping(value="/v1/libraryEvent", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<?> putLibraryEvent(@RequestBody LibraryEvent event) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException{
		
		event.setLibraryEventType(LibraryEventType.UPDATE);
		SendResult<Integer, String> result =  eventProducer.sendLibraryEvent_Approach2(event);
		return ResponseEntity.status(HttpStatus.OK).body(event);
	}
}
