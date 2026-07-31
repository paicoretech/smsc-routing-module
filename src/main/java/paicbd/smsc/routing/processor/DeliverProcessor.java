package paicbd.smsc.routing.processor;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaConsumerConstants;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.Watcher;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeliverProcessor implements RoutingProcessor {
    private final AtomicInteger processedDeliveries = new AtomicInteger(0);
    private final CommonProcessor commonProcessor;

    @PostConstruct
    public void init() {
        log.info("Deliver Processor Initialized Successfully");
        Thread.startVirtualThread(() -> new Watcher("Routing:PreDeliver", processedDeliveries, 5));
    }

    @KafkaListener(
            topics = KafkaTopicsConstants.PRE_DELIVER_TOPIC,
            groupId = KafkaConsumerConstants.ROUTING_DELIVERY_GROUP_ID)
    public void processMessage(List<String> messages) {
        Flux.fromIterable(messages)
                .map(x -> Converter.stringToObject(x, MessageEvent.class))
                .doOnNext(event -> {
                    prepareMessage(event);
                    processedDeliveries.incrementAndGet();
                })
                .subscribe();
    }

    @Override
    public void prepareMessage(MessageEvent messageEvent) {
        this.commonProcessor.setUpInitialSettings(messageEvent);
        this.commonProcessor.processDlrInAsync(messageEvent);
    }
}
