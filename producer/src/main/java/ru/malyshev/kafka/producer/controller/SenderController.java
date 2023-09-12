package ru.malyshev.kafka.producer.controller;

import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.ResponseEntity.status;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.malyshev.kafka.producer.model.Message;
import ru.malyshev.kafka.producer.service.MessageSender;

@RestController
@RequiredArgsConstructor
@RequestMapping
public class SenderController {

  private final MessageSender messageSender;

  @PostMapping("/message/send")
  public ResponseEntity<String> send(
      @RequestParam(value = "part", required = false) Integer partition,
      @RequestParam(value = "topic") String topicName,
      @RequestBody Message message) {
    if (messageSender.send(message.getMessageText(), topicName, partition)) {
      return ResponseEntity.ok("ok");
    }
    return status(INTERNAL_SERVER_ERROR).body("kafka is not available");
  }

}