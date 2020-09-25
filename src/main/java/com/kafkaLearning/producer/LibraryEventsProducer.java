package com.kafkaLearning.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaLearning.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventsProducer {
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	ObjectMapper objectMapper;
	
	public SendResult<Integer, String> sendLibraryEventSynchronoss(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		SendResult<Integer, String> sendResult = null;
		try {
			sendResult = kafkaTemplate.send("library-events",key, value).get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			log.error("InterruptedException/ExecutionException while sending the message & exception is {}", e.getMessage());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("Exception while sending the message & exception is {}", e.getMessage());
		}
		return sendResult;
	}
	
	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		//read topic from application.properties
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send("library-events",key, value);
		listenableFutureCallback(key, value, listenableFuture);
	}
	
	public ListenableFuture<SendResult<Integer, String>> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		ProducerRecord<Integer, String> producerRecord = createProducerRecord(key, value);
		//read topic from application.properties
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
		listenableFutureCallback(key, value, listenableFuture);
		return listenableFuture;
	}
	
	private ProducerRecord<Integer, String> createProducerRecord(Integer key, String value) {
		// Header to pass additional information & consumer can consume based on header
		List<Header> recordHeader = new ArrayList<>();
		recordHeader.add(new RecordHeader("event-source", "scanner".getBytes()));
		recordHeader.add(new RecordHeader("event-source", "mannual".getBytes()));
		
		return new ProducerRecord<Integer, String>("library-events", null, key, value, recordHeader);
	}

	private void listenableFutureCallback(Integer key, String value, ListenableFuture<SendResult<Integer, String>> listenableFuture) {
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				// TODO Auto-generated method stub
				handleSuccess(key, value, result);
			}


			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
				
			}
		
		});
	}

	protected void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error while sending the message & exception is {}", ex.getMessage());
		
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		// TODO Auto-generated method stub
		log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
		log.info("RecordMeta ="+result.getRecordMetadata().toString());
	}
}
