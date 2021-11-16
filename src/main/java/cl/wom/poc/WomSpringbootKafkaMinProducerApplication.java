package cl.wom.poc;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootApplication
public class WomSpringbootKafkaMinProducerApplication {

	/**
	 * Log de la clase.
	 */
	private static Logger log = LogManager.getLogger(WomSpringbootKafkaMinProducerApplication.class);

	public static final String TOPIC = "poc-test01";

	public static void main(String[] args) {
		SpringApplication.run(WomSpringbootKafkaMinProducerApplication.class, args);
	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name(TOPIC).partitions(10).replicas(1).build();
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {

		Map<String, Object> producerProps = new HashMap<>();

		// Kafka broker
		producerProps.put("bootstrap.servers", "localhost:9092");

		// Deserialization
		producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		return new DefaultKafkaProducerFactory<>(producerProps);

	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public ApplicationRunner runner(KafkaTemplate<String, String> kafkaTemplate) {
		return args -> {
			while (true) {
				ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(TOPIC, new Date().toString());

				future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
					@Override
					public void onSuccess(SendResult<String, String> result) {
						RecordMetadata metadata = result.getRecordMetadata();
						ProducerRecord<String, String> record = result.getProducerRecord();

						log.info("[runner] value = {}, Partition = {}, Offset = {}, ", record.value(),
								metadata.partition(), metadata.offset());
					}

					@Override
					public void onFailure(Throwable e) {
						log.error("[runner][Throwable] Error={}", e);
						throw new KafkaException(e);
					}
				});
				Thread.sleep(500);
			}
		};
	}

}
