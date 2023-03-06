package com.example.consumer.config;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

@Service
@RequiredArgsConstructor
public class KafkaRedisService {

  private final Jedis jedis;

  @KafkaListener(topics = "my-topic", groupId = "my-group")
  public void processRecord(@NonNull ConsumerRecord<String, Integer> record) {
    int number = record.value();
    String key = "number:" + number;
    jedis.incr(key);
  }
}
