package paicbd.smsc.routing.processor;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.Watcher;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class MessageProcessorTest {
    @Mock
    private CommonProcessor commonProcessor;

    @InjectMocks
    private MessageProcessor messageProcessor;

    @Test
    void initAddsThreeWatchers() {
        assertEquals(0, messageProcessor.getWatchers().size());

        messageProcessor.init();

        assertEquals(3, messageProcessor.getWatchers().size());
        messageProcessor.getWatchers().forEach(Assertions::assertNotNull);
    }

    @Test
    void shutdownStopsAllWatchers() {
        Watcher watcher1 = mock(Watcher.class);
        Watcher watcher2 = mock(Watcher.class);
        Watcher watcher3 = mock(Watcher.class);

        messageProcessor.getWatchers().addAll(List.of(watcher1, watcher2, watcher3));

        messageProcessor.shutdown();

        verify(watcher1, times(1)).stopWatching();
        verify(watcher2, times(1)).stopWatching();
        verify(watcher3, times(1)).stopWatching();
    }

    @Test
    void processHighPriorityMessage_invokesCommonProcessor() {
        MessageEvent event = createTestMessageEvent();
        List<String> messages = List.of(Converter.valueAsString(event));

        messageProcessor.processHighPriorityMessage(messages);

        Awaitility.await().atMost(Duration.ofMillis(500)).untilAsserted(() -> {
            verify(commonProcessor, times(1)).setUpInitialSettings(any(MessageEvent.class));
            verify(commonProcessor, times(1)).processMessage(any(MessageEvent.class));
            verify(commonProcessor, never()).processDlrInAsync(any(MessageEvent.class));
        });
    }

    @Test
    void processMediumPriorityMessage_invokesCommonProcessor() {
        MessageEvent event = createTestMessageEvent();
        List<String> messages = List.of(Converter.valueAsString(event));

        messageProcessor.processMediumPriorityMessage(messages);

        Awaitility.await().atMost(Duration.ofMillis(500)).untilAsserted(() -> {
            verify(commonProcessor, times(1)).setUpInitialSettings(any(MessageEvent.class));
            verify(commonProcessor, times(1)).processMessage(any(MessageEvent.class));
        });
    }

    @Test
    void processLowPriorityMessage_invokesCommonProcessor() {
        MessageEvent event = createTestMessageEvent();
        List<String> messages = List.of(Converter.valueAsString(event));

        messageProcessor.processLowPriorityMessage(messages);

        Awaitility.await().atMost(Duration.ofMillis(500)).untilAsserted(() -> {
            verify(commonProcessor, atLeastOnce()).setUpInitialSettings(any(MessageEvent.class));
            verify(commonProcessor, times(1)).processMessage(any(MessageEvent.class));
        });
    }

    @Test
    void processHighPriorityMessageDlrMessageRoutesDlrAsync() {
        MessageEvent event = createTestMessageEvent();
        List<String> messages = List.of(Converter.valueAsString(event));

        messageProcessor.processHighPriorityMessage(messages);

        Awaitility.await().atMost(Duration.ofMillis(500)).untilAsserted(() -> {
            verify(commonProcessor, atLeastOnce()).setUpInitialSettings(any(MessageEvent.class));
            verify(commonProcessor, never()).processDlrInAsync(any(MessageEvent.class));
            verify(commonProcessor, times(1)).processMessage(any(MessageEvent.class));
        });
    }

    @Test
    void processBatchMessages_processesAllMessages() {
        MessageEvent event1 = createTestMessageEvent();
        MessageEvent event2 = createTestMessageEvent();
        List<String> messages = List.of(
                Converter.valueAsString(event1),
                Converter.valueAsString(event2)
        );

        messageProcessor.processHighPriorityMessage(messages);

        Awaitility.await().atMost(Duration.ofMillis(500)).untilAsserted(() -> {
            verify(commonProcessor, times(2)).setUpInitialSettings(any(MessageEvent.class));
            verify(commonProcessor, times(2)).processMessage(any(MessageEvent.class));
        });
    }

    @Test
    void testPrepareMessageTreatLikeDlr() {
        MessageEvent messageEvent = new MessageEvent();
        messageEvent.setCheckSubmitSmResponse(true);

        messageProcessor.prepareMessage(messageEvent);

        verify(commonProcessor, times(1)).setUpInitialSettings(messageEvent);
        verify(commonProcessor, never()).processDlrInAsync(messageEvent);
        verify(commonProcessor, times(1)).processMessage(messageEvent);
    }

    @Test
    void testPrepareMessageTreatLikeMessage() {
        MessageEvent messageEvent = new MessageEvent();
        messageEvent.setCheckSubmitSmResponse(false);

        messageProcessor.prepareMessage(messageEvent);

        verify(commonProcessor, times(1)).setUpInitialSettings(messageEvent);
        verify(commonProcessor, never()).processDlrInAsync(messageEvent);
        verify(commonProcessor, times(1)).processMessage(messageEvent);
    }

    private MessageEvent createTestMessageEvent() {
        return MessageEvent.builder()
                .id(System.currentTimeMillis() + "-" + System.nanoTime())
                .messageId(System.currentTimeMillis() + "-" + System.nanoTime())
                .systemId("test_sp")
                .sourceAddr("50588888888")
                .destinationAddr("50599999999")
                .shortMessage("Test message")
                .originNetworkType("SP")
                .originProtocol("SMPP")
                .originNetworkId(1)
                .checkSubmitSmResponse(false)
                .build();
    }
}