package com.kafkaLearning.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Mockito.*;
import org.mockito.Spy;
import org.mockito.internal.matchers.Any;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaLearning.domain.Book;
import com.kafkaLearning.domain.LibraryEvent;
import com.kafkaLearning.domain.LibraryEventType;

@ExtendWith(MockitoExtension.class)
public class LibrarayEventProducerUnitTest {
	
	@InjectMocks
	LibraryEventsProducer eventProducer;
	
	@Spy
	ObjectMapper objectMapper;
	
	
	@Mock
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Test
	public void approach2_test_failure() throws JsonProcessingException {
		Book book = Book.builder().bookId(505).
				bookName("Spring Kafka").bookAuthor("Kumar Nagaraju").build();
		
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).
				book(book).libraryEventType(LibraryEventType.NEW).build();
		SettableListenableFuture future = new SettableListenableFuture<>();
		future.setException(new RuntimeException("Calling Kafka"));
		Mockito.when(kafkaTemplate.send(Mockito.any(ProducerRecord.class))).thenReturn(future);
		assertThrows(Exception.class, ()->eventProducer.sendLibraryEvent_Approach2(libraryEvent).get());
	}
	
	@Test
	public void approach2_test_Success() throws JsonProcessingException, InterruptedException, ExecutionException {
		Book book = Book.builder().bookId(505).
				bookName("Spring Kafka").bookAuthor("Kumar Nagaraju").build();
		
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).
				book(book).libraryEventType(LibraryEventType.NEW).build();
		ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>
		("library-events", libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent));
		RecordMetadata recordMetaData = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, 342, System.currentTimeMillis(), 1, 2);
		SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetaData);
		SettableListenableFuture future = new SettableListenableFuture<>();
		future.set(sendResult);
		Mockito.when(kafkaTemplate.send(Mockito.any(ProducerRecord.class))).thenReturn(future);
		SendResult<Integer, String> response = eventProducer.sendLibraryEvent_Approach2(libraryEvent).get();
		assertEquals(1, response.getRecordMetadata().partition());
	}
}
