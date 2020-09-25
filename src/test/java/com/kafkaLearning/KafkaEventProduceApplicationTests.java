package com.kafkaLearning;

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
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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

import com.fasterxml.jackson.databind.Module.SetupContext;
import com.google.gson.Gson;
import com.kafkaLearning.domain.Book;
import com.kafkaLearning.domain.LibraryEvent;
import com.kafkaLearning.domain.LibraryEventType;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics= {"library-events"}, partitions=3)
@TestPropertySource(properties= {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
class KafkaEventProduceApplicationTests {

	@Autowired
	TestRestTemplate restTemplate; 
	
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	
	private Consumer<Integer, String> consumer;
	
	@BeforeEach
	void setUp() {
		Map<String, Object> config = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
		consumer = new DefaultKafkaConsumerFactory(config, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
	}
	
	@AfterEach
	void tearDown() {
		consumer.close();
	}

	@Test
	@Timeout(5)
	void postLibraryEvent() {
		Gson g = new Gson();
		Book book = Book.builder().bookId(505).
				bookName("Spring Kafka").bookAuthor("Kumar Nagaraju").build();
		
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).
				book(book).libraryEventType(LibraryEventType.NEW).build();
		String libraryEventStr = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":505,\"bookName\":\"Spring Kafka\",\"bookAuthor\":\"Kumar Nagaraju\"}}";
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<LibraryEvent> httpEntity = new HttpEntity<LibraryEvent>(libraryEvent,headers);
		ResponseEntity<LibraryEvent> reponseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST, httpEntity, LibraryEvent.class);
		
		assertEquals(HttpStatus.CREATED, reponseEntity.getStatusCode());
		ConsumerRecord<Integer, String> consumerRecord =  KafkaTestUtils.getSingleRecord(consumer, "library-events");
		String value = consumerRecord.value();
		assertEquals(libraryEventStr, value);
	}
	
	@Test
	@Timeout(5)
	void updateLibraryEvent() {
		Gson g = new Gson();
		Book book = Book.builder().bookId(505).
				bookName("Spring Kafka").bookAuthor("Kumar Nagaraju").build();
		
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(456).
				book(book).libraryEventType(LibraryEventType.UPDATE).build();
		String libraryEventStr = "{\"libraryEventId\":456,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":505,\"bookName\":\"Spring Kafka\",\"bookAuthor\":\"Kumar Nagaraju\"}}";
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<LibraryEvent> httpEntity = new HttpEntity<LibraryEvent>(libraryEvent,headers);
		ResponseEntity<LibraryEvent> reponseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, httpEntity, LibraryEvent.class);
		
		assertEquals(HttpStatus.OK, reponseEntity.getStatusCode());
		ConsumerRecord<Integer, String> consumerRecord =  KafkaTestUtils.getSingleRecord(consumer, "library-events");
		String value = consumerRecord.value();
		assertEquals(libraryEventStr, value);
	}

}
