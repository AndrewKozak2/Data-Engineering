package com.example.lab4;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic topic1() {
        return TopicBuilder.name("Topic1").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic topic2() {
        return TopicBuilder.name("Topic2").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic statsTopic() {
        return TopicBuilder.name("trip-stats-daily").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic popularStationTopic() {
        return TopicBuilder.name("popular-start-station").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic topStationsTopic() {
        return TopicBuilder.name("top-stations").partitions(1).replicas(1).build();
    }
}
