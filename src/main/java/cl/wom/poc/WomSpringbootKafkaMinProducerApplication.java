package cl.wom.poc;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@SpringBootApplication
public class WomSpringbootKafkaMinProducerApplication {

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

// TODO NO FUNCIONA
//	@Scheduled(fixedDelay = 500, initialDelay = 1000)
//	public void send(KafkaTemplate<String, String> kafkaTemplate) {
//		kafkaTemplate.send(TOPIC, new Date().toString());
//	}

	@Bean
	public ApplicationRunner runner(KafkaTemplate<String, String> kafkaTemplate) {
		return args -> {
			while (true) {
				kafkaTemplate.send(TOPIC, new Date().toString());
				Thread.sleep(500);
			}
		};
	}

}
