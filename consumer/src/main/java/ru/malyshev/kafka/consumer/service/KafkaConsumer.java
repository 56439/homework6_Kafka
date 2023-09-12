package ru.malyshev.kafka.consumer.service;

import static org.apache.kafka.common.utils.Utils.sleep;
import static ru.malyshev.kafka.consumer.config.KafkaConsumerConfig.TOPIC_NAME;
import static ru.malyshev.kafka.consumer.config.KafkaConsumerConfig.TOPIC_NAME_PARTITIONS;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaConsumer {

  @KafkaListener(topics = TOPIC_NAME)
  public void listenTopic(String message) {
    log.info("Receive new message! {}", message);
  }

  @KafkaListener(topics = TOPIC_NAME_PARTITIONS)
  public void listenTopicPartitions(
      @Payload String message,
      @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
      @Header(KafkaHeaders.RECEIVED_PARTITION) String receivedPartition,
      @Header(KafkaHeaders.OFFSET) String receivedOffset
  ) {
    sleep(2000);
    log.info("Receive from Topic {} by partition {}, offset {}: {}", receivedTopic,
        receivedPartition, receivedOffset, message);
  }

  /*@KafkaListener(topics = TOPIC_NAME_PARTITIONS,
      topicPartitions = @TopicPartition(
          topic = TOPIC_NAME_PARTITIONS,
          partitions = {"0", "1"}
      ))
  public void listen2(String message) {
    log.info("Receive new message! {}", message);
  }*/
}