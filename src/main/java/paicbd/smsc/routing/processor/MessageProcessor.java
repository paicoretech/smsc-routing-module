package paicbd.smsc.routing.processor;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaConsumerConstants;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.Watcher;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageProcessor implements RoutingProcessor {
    private final AtomicInteger processedHighPriority = new AtomicInteger(0);
    private final AtomicInteger processedMediumPriority = new AtomicInteger(0);
    private final AtomicInteger processedLowPriority = new AtomicInteger(0);
    private final CommonProcessor commonProcessor;

    @Getter
    private final List<Watcher> watchers = new ArrayList<>();

    @PostConstruct
    public void init() {
        log.info("Message Processor Initialized Successfully");
        this.watchers.add(new Watcher("Routing:PreMessage:High", processedHighPriority, 1));
        this.watchers.add(new Watcher("Routing:PreMessage:Medium", processedMediumPriority, 1));
        this.watchers.add(new Watcher("Routing:PreMessage:Low", processedLowPriority, 1));
    }

    @KafkaListener(
            topics = KafkaTopicsConstants.PRE_MESSAGE_HIGH_TOPIC,
            groupId = KafkaConsumerConstants.ROUTING_HIGH_MESSAGE_GROUP_ID,
            containerFactory = "highPriorityKafkaListenerContainerFactory")
    public void processHighPriorityMessage(List<String> messages) {
        processMessages(messages, processedHighPriority);
    }

    @KafkaListener(
            topics = KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC,
            groupId = KafkaConsumerConstants.ROUTING_MEDIUM_MESSAGE_GROUP_ID,
            containerFactory = "mediumPriorityKafkaListenerContainerFactory")
    public void processMediumPriorityMessage(List<String> messages) {
        processMessages(messages, processedMediumPriority);
    }

    @KafkaListener(
            topics = KafkaTopicsConstants.PRE_MESSAGE_LOW_TOPIC,
            groupId = KafkaConsumerConstants.ROUTING_LOW_MESSAGE_GROUP_ID,
            containerFactory = "lowPriorityKafkaListenerContainerFactory")
    public void processLowPriorityMessage(List<String> messages) {
        processMessages(messages, processedLowPriority);
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Message Processor watchers.");
        for (Watcher watcher : watchers) {
            watcher.stopWatching();
        }
    }

    private void processMessages(List<String> messages, AtomicInteger counter) {
        Flux.fromIterable(messages)
                .map(x -> Converter.stringToObject(x, MessageEvent.class))
                .doOnNext(event -> {
                    prepareMessage(event);
                    counter.incrementAndGet();
                })
                .subscribe();
    }

    @Override
    public void prepareMessage(MessageEvent messageEvent) {
        this.commonProcessor.setUpInitialSettings(messageEvent);
        this.commonProcessor.processMessage(messageEvent);
    }
}
