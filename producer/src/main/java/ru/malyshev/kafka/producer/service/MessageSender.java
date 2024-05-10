package ru.malyshev.kafka.producer.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageSender {

  private static final Random RND = new Random();

  private static final List<String> KEYS = new ArrayList<>() {{
    add("FIRST");
    add("SECOND");
    add("MAIN");
  }};

  private final KafkaTemplate<String, String> template;

  public boolean send(String message, String topicName, Integer partition) {
    val key = KEYS.get(RND.nextInt(KEYS.size()));
    val future = template.send(
        topicName,
        partition == null ? 0 : partition,
        key,
        message);

    try {
      SendResult<String, String> result = future.get(2, TimeUnit.SECONDS);
      log.info("Successful send to {} by key {} with offset {} to partition {}",
          result.getProducerRecord().topic(),
          key,
          result.getRecordMetadata().offset(),
          result.getRecordMetadata().partition());
      return true;
    } catch (TimeoutException | ExecutionException | InterruptedException e) {
      log.error("Cannot send message to Kafka", e);
    }
    return false;
  }

/*  @KafkaListener(topics = TOPIC_NAME,
      topicPartitions = @TopicPartition(
          topic = TOPIC_NAME,
          partitionOffsets = @PartitionOffset(
              partition = "0", initialOffset = "0"
          )))
  public void consume(String message) {
    log.info("Receive new message! {}", message);
  }*/


}
