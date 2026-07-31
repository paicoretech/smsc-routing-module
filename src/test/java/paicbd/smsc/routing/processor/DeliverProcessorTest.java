package paicbd.smsc.routing.processor;


import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.utils.Converter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import paicbd.smsc.routing.util.AppProperties;
import redis.clients.jedis.JedisCluster;

import java.time.Duration;
import java.util.List;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DeliverProcessorTest {
    @Mock
    JedisCluster jedisCluster;

    @Mock
    AppProperties appProperties;

    @Mock
    CommonProcessor commonProcessor;

    @InjectMocks
    DeliverProcessor deliverProcessor;

    @Test
    void processMessageProcessesBatchAndRoutesByMessageType() {
        MessageEvent regularMessage = new MessageEvent();
        regularMessage.setCheckSubmitSmResponse(false);

        MessageEvent dlrMessage = new MessageEvent();
        dlrMessage.setCheckSubmitSmResponse(true);

        List<String> messages = List.of(
                Converter.valueAsString(regularMessage),
                Converter.valueAsString(dlrMessage)
        );

        deliverProcessor.processMessage(messages);

        Awaitility.await().atMost(Duration.ofMillis(700)).untilAsserted(() -> {
            verify(commonProcessor, times(2)).setUpInitialSettings(org.mockito.ArgumentMatchers.any(MessageEvent.class));
            verify(commonProcessor, never()).processMessage(org.mockito.ArgumentMatchers.any(MessageEvent.class));
        });
    }

    @Test
    void prepareMessageTreatLikeMessage() {
        MessageEvent messageEvent = new MessageEvent();
        messageEvent.setCheckSubmitSmResponse(false);

        deliverProcessor.prepareMessage(messageEvent);

        verify(commonProcessor, times(1)).setUpInitialSettings(messageEvent);
        verify(commonProcessor, never()).processMessage(messageEvent);
        verify(commonProcessor, times(1)).processDlrInAsync(messageEvent);
    }

    @Test
    void prepareMessageTreatLikeDlr() {
        MessageEvent messageEvent = new MessageEvent();
        messageEvent.setCheckSubmitSmResponse(true);

        deliverProcessor.prepareMessage(messageEvent);

        verify(commonProcessor, times(1)).setUpInitialSettings(messageEvent);
        verify(commonProcessor, never()).processMessage(messageEvent);
        verify(commonProcessor, times(1)).processDlrInAsync(messageEvent);
    }
}
