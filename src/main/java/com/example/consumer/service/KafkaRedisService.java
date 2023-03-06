package com.example.consumer.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class KafkaRedisService {

  private final RedisTemplate<String, String> redisTemplate;

  @KafkaListener(topics = "${spring.kafka.topic}", groupId = "${spring.kafka.groupId}")
  public void processMessage(ConsumerRecord<String, String> consumerRecord) {
    String value = consumerRecord.value();

    String key = "number:" + value;
    Long count = redisTemplate.opsForValue().increment(key);

    log.info("Number " + value + " occurred " + count + " times.");
  }
}
