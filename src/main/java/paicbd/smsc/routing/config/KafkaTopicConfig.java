package paicbd.smsc.routing.config;

import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import paicbd.smsc.routing.util.AppProperties;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {

    private final AppProperties appProperties;


    @Bean
    public NewTopic highPriorityTopic() {
        return TopicBuilder.name(KafkaTopicsConstants.PRE_MESSAGE_HIGH_TOPIC)
                .partitions(appProperties.getKafkaHighPriorityConcurrency())
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic mediumPriorityTopic() {
        return TopicBuilder.name(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC)
                .partitions(appProperties.getKafkaMediumPriorityConcurrency())
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic lowPriorityTopic() {
        return TopicBuilder.name(KafkaTopicsConstants.PRE_MESSAGE_LOW_TOPIC)
                .partitions(appProperties.getKafkaLowPriorityConcurrency())
                .replicas(1)
                .build();
    }
}