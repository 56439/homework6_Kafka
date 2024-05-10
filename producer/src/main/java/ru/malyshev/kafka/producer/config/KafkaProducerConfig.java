package ru.malyshev.kafka.producer.config;

import java.util.HashMap;
import lombok.val;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaProducerConfig {

  public static final String TOPIC_NAME = "kafka.my.topic";
  public static final String TOPIC_NAME_PARTITIONS = TOPIC_NAME + ".partitions";

  @Value("${spring.kafka.bootstrap-servers}")
  private String serverAddress;


  @Bean
  public NewTopic kafkaExampleTopic() {
    return TopicBuilder.name(TOPIC_NAME)
        .replicas(1)
        .build();
  }

  @Bean
  public NewTopic kafkaMessageTopic() {
    return TopicBuilder.name(TOPIC_NAME_PARTITIONS)
        .replicas(1)
        .partitions(10)
        .build();
  }

  @Bean
  public ProducerFactory<String, String> producerFactory() {

    val config = new HashMap<String, Object>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddress);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

    return new DefaultKafkaProducerFactory<>(config);
  }

  @Bean
  public KafkaTemplate<String, String> kafkaStringTemplate(
      ProducerFactory<String, String> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }

}
