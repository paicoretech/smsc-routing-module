package paicbd.smsc.routing.processor;

import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessageRegister;
import com.paicbd.smsc.dto.RoutingRule;
import com.paicbd.smsc.dto.SipGateways;
import com.paicbd.smsc.dto.Ss7Settings;
import com.paicbd.smsc.dto.Udh;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.dto.diameter.DiameterConfig;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.kafka.KafkaUtils;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RegEventState;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.utils.SmppUtils;
import com.paicbd.smsc.utils.UtilsEnum;
import org.jsmpp.bean.OptionalParameter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import paicbd.smsc.routing.component.CreditHandler;
import paicbd.smsc.routing.component.RoutingMatcher;
import paicbd.smsc.routing.loaders.SettingsLoader;
import paicbd.smsc.routing.util.AppProperties;
import paicbd.smsc.routing.util.StaticMethods;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.paicbd.smsc.utils.GeneralSmscConstants.USE_DND_FILTERING;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommonProcessorTest {
    @Mock
    SettingsLoader settingsLoader;

    @Mock
    RoutingMatcher routingMatcher;

    @Mock
    ConcurrentMap<Integer, Gateway> gateways;

    @Mock
    ConcurrentMap<Integer, com.paicbd.smsc.dto.ServiceProvider> serviceProviders;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    ScyllaManager scyllaManager;

    @Mock
    ConcurrentMap<Integer, DiameterConfig> diameterGatewayMap;

    @Mock
    AppProperties appProperties;

    @Mock
    CreditHandler creditHandler;

    @Mock
    ConcurrentMap<Integer, SipGateways> sipGatewaysConcurrentMap;

    @InjectMocks
    CommonProcessor commonProcessor;

    @BeforeEach
    void setUp() {
        commonProcessor = new CommonProcessor(creditHandler, settingsLoader,
                routingMatcher, gateways, serviceProviders, scyllaManager, kafkaTemplate, diameterGatewayMap, appProperties, sipGatewaysConcurrentMap);
    }


    @ParameterizedTest
    @MethodSource("getMessageToTestInitialSettings")
    @DisplayName("Test SetUpInitialSettings and set the values of generalSetting to the MessageEvent")
    void setUpInitialSettingsAndSetTheValuesOfGeneralSettingToTheMessageEvent(MessageEvent messageEvent) {
        GeneralSettings generalSettings = GeneralSettings.builder()
                .validityPeriod(120)
                .maxValidityPeriod(240)
                .id(1)
                .destAddrNpi(1)
                .destAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddrTon(1)
                .encodingGsm7(0)
                .encodingUcs2(2)
                .encodingIso88591(3)
                .build();

        when(settingsLoader.getSmppHttpSettings()).thenReturn(generalSettings);

        commonProcessor.setUpInitialSettings(messageEvent);

        if (Objects.isNull(messageEvent.getSourceAddrTon()))
            assertEquals(generalSettings.getSourceAddrTon(), messageEvent.getSourceAddrTon());

        if (Objects.isNull(messageEvent.getSourceAddrNpi()))
            assertEquals(generalSettings.getSourceAddrNpi(), messageEvent.getSourceAddrNpi());

        if (Objects.isNull(messageEvent.getDestAddrNpi()))
            assertEquals(generalSettings.getDestAddrNpi(), messageEvent.getDestAddrNpi());

        if (Objects.isNull(messageEvent.getDestAddrTon()))
            assertEquals(generalSettings.getDestAddrTon(), messageEvent.getDestAddrTon());

        if (messageEvent.getValidityPeriod() == 0)
            assertEquals(generalSettings.getValidityPeriod(), messageEvent.getValidityPeriod());

        assertNotNull(messageEvent.getStringValidityPeriod());
        assertTrue(messageEvent.getValidityPeriod() > 0);
    }

    static Stream<MessageEvent> getMessageToTestInitialSettings() {
        return Stream.of(
                MessageEvent.builder()
                        .id("1722442489766-7788604799226")
                        .messageId("1722442489770-7788608933795")
                        .systemId("http_sp")
                        .commandStatus(0)
                        .segmentSequence(2)
                        .sourceAddr("50588888888")
                        .destinationAddr("50599999999")
                        .validityPeriod(0)
                        .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                        .shortMessage("Hello ..!")
                        .originNetworkType("SP")
                        .originProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                        .originNetworkId(1)
                        .destNetworkType("GW")
                        .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                        .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                        .destNetworkId(4)
                        .build(),

                MessageEvent.builder()
                        .id("1722442489766-7788604799226")
                        .messageId("1722442489770-7788608933795")
                        .systemId("smpp_sp")
                        .commandStatus(0)
                        .segmentSequence(2)
                        .sourceAddr("50588888888")
                        .destinationAddr("50599999999")
                        .validityPeriod(60)
                        .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                        .dataCoding(0)
                        .shortMessage("Hello ..!")
                        .originNetworkType("SP")
                        .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                        .originNetworkId(1)
                        .destNetworkType("GW")
                        .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                        .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                        .destNetworkId(4)
                        .destAddrNpi(1)
                        .destAddrTon(1)
                        .sourceAddrNpi(1)
                        .sourceAddrTon(1)
                        .esmClass(3)
                        .build(),

                MessageEvent.builder()
                        .id("1722442489766-7788604799226")
                        .messageId("1722442489770-7788608933795")
                        .systemId("smpp_sp")
                        .commandStatus(0)
                        .segmentSequence(2)
                        .sourceAddr("50588888888")
                        .destinationAddr("50599999999")
                        .validityPeriod(60)
                        .stringValidityPeriod("000000000200000R")
                        .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                        .dataCoding(0)
                        .shortMessage("Hello ..!")
                        .originNetworkType("SP")
                        .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                        .originNetworkId(1)
                        .destNetworkType("GW")
                        .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                        .smscMessagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                        .destNetworkId(4)
                        .destAddrNpi(1)
                        .destAddrTon(1)
                        .sourceAddrNpi(1)
                        .sourceAddrTon(1)
                        .esmClass(3)
                        .build()
        );
    }

    @Test
    @DisplayName("Test BroadcastSms for any DataCoding")
    void testSmsFromBroadcast() {
        when(settingsLoader.getSmppHttpSettings()).thenReturn(mock(GeneralSettings.class));
        RoutingRule broadcastRoutingRule = getRuleWithActionsAndSmppDestination();
        when(routingMatcher.getRouting(any())).thenReturn(broadcastRoutingRule);
        when(routingMatcher.getDestinationByRouting(broadcastRoutingRule)).thenReturn(broadcastRoutingRule.getDestination().getFirst());
        gateways = new ConcurrentHashMap<>();
        Gateway gateway = Gateway.builder()
                .networkId(2)
                .name("GW")
                .splitSmppType("udh")
                .encodingGsm7(EncodingUtils.GSM7)
                .encodingUcs2(EncodingUtils.UCS2)
                .encodingIso88591(EncodingUtils.ISO88591)
                .requestDLR(RequestDelivery.REQUEST_DLR.getValue())
                .build();

        gateways.put(2, gateway);
        commonProcessor = new CommonProcessor(creditHandler, settingsLoader,
                routingMatcher, gateways, serviceProviders, scyllaManager, kafkaTemplate, diameterGatewayMap, appProperties, sipGatewaysConcurrentMap);
        MessageEvent eventDc4 = MessageEvent.builder()
                .id("1747167540021-14605739631291")
                .broadcastId(1)
                .originNetworkId(1)
                .messageId("1747167495135-14560853133653")
                .systemId("broadcast")
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .sourceAddr("1322888094")
                .destinationAddr("123452")
                .shortMessage("46524F4D2042524F414443415354")
                .esmClass(0)
                .dataCoding(4)
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                .parentId("1747167495135-14560853133653")
                .smscMessagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .build();
        commonProcessor.processMessage(eventDc4);
        assertEquals(0, eventDc4.getEsmClass());
        assertNotNull(eventDc4.getMessageBytes());
        assertEquals(0, eventDc4.getUdhLength());
        assertTrue(EncodingUtils.isHexadecimal(eventDc4.getShortMessage()));

        MessageEvent eventDc19 = MessageEvent.builder()
                .id("1747167540021-14605739361693")
                .broadcastId(1)
                .originNetworkId(1)
                .messageId("1747167495135-14560853073960")
                .systemId("broadcast")
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .sourceAddr("1322888094")
                .destinationAddr("123454")
                .shortMessage("Hello!")
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .dataCoding(19)
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                .parentId("1747167495135-14560853073960")
                .build();

        commonProcessor.processMessage(eventDc19);
        assertEquals(3, eventDc19.getEsmClass());
        assertNotNull(eventDc19.getMessageBytes());
        assertEquals(0, eventDc19.getUdhLength());

        MessageEvent eventDc0 = MessageEvent.builder()
                .originNetworkId(1)
                .broadcastId(1)
                .id("1747167540021-14605739361693")
                .messageId("1747167495135-14560853073960")
                .systemId("broadcast")
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .sourceAddr("1322888094")
                .destinationAddr("123454")
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .shortMessage("Hello ..!")
                .dataCoding(0)
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                .parentId("1747167495135-14560853073960")
                .build();

        commonProcessor.processMessage(eventDc0);
        assertEquals(3, eventDc0.getEsmClass());
        assertNotNull(eventDc0.getMessageBytes());
        assertEquals(0, eventDc0.getUdhLength());
        assertFalse(EncodingUtils.isHexadecimal(eventDc0.getShortMessage()));

        commonProcessor.processMessagePayloadTlvIfApply(eventDc0);

    }

    @Test
    @DisplayName("Test processMessage without rule then it write fail cdr")
    void processMessageWithoutRuleThenItWriteFailCdr() {
        MessageEvent messageEvent = MessageEvent.builder()
                .id("1722442489766-7788604799226")
                .messageId("1722442489770-7788608933795")
                .systemId("smpp_sp")
                .commandStatus(0)
                .segmentSequence(2)
                .sourceAddr("50588888888")
                .destinationAddr("50599999999")
                .validityPeriod(60)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                .dataCoding(0)
                .shortMessage("Hello ..!")
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .originNetworkId(1)
                .destNetworkType("GW")
                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .destNetworkId(4)
                .destAddrNpi(1)
                .destAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddrTon(1)
                .esmClass(3)
                .build();
        when(routingMatcher.getRouting(any())).thenReturn(null);
        assertDoesNotThrow(() -> commonProcessor.processMessage(messageEvent));
    }

    @Test
    @DisplayName("Test processMessage without destination then it write fail cdr")
    void processMessageWithoutDestinationThenItWriteFailCdr() {
        MessageEvent messageEvent = MessageEvent.builder()
                .id("1722442489766-7788604799226")
                .messageId("1722442489770-7788608933795")
                .systemId("smpp_sp")
                .commandStatus(0)
                .segmentSequence(2)
                .sourceAddr("50588888888")
                .destinationAddr("50599999999")
                .validityPeriod(60)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                .dataCoding(0)
                .shortMessage("Hello ..!")
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .originNetworkId(1)
                .destNetworkType("GW")
                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .destNetworkId(4)
                .destAddrNpi(1)
                .destAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddrTon(1)
                .esmClass(3)
                .build();

        RoutingRule routingRule = RoutingRule.builder()
                .id(1)
                .originNetworkId(1)
                .destination(List.of())
                .build();

        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(null);
        assertDoesNotThrow(() -> commonProcessor.processMessage(messageEvent));
    }

    @ParameterizedTest
    @MethodSource("provideTestCasesForProcessMessage")
    @DisplayName("Test processMessage with rule and destination then it enqueues the message")
    void processMessageWithRuleAndDestinationThenItEnqueuesTheMessage(MessageEvent messageEvent, RoutingRule routingRule, Gateway gateway,
                                                                      Ss7Settings ss7Settings, MessageRegister registerData, byte[] expectedMessageBytes) {
        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        RoutingRule.Destination selectedDestination = routingRule.getDestination().stream()
                .min(Comparator.comparingInt(RoutingRule.Destination::getPriority))
                .orElse(null);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(selectedDestination);

        if (Objects.nonNull(ss7Settings)) {
            when(appProperties.getTlvSccpCalledAddrSri()).thenReturn((short) 5);
            when(settingsLoader.getSs7Settings(anyInt())).thenReturn(ss7Settings);
            messageEvent.setDestProtocol(GeneralSmscConstants.SS7_PROTOCOL);
            messageEvent.setDestNetworkType("GW");
            // Hex: 39 = ASCII : 71 = CHAR: 9
            UtilsRecords.OptionalParameter optionalParameter = new UtilsRecords.OptionalParameter((short) 5, "39");
            messageEvent.setOptionalParameters(Collections.singletonList(optionalParameter));
        }

        GeneralSettings generalSettings = new GeneralSettings();
        when(settingsLoader.getSmppHttpSettings()).thenReturn(generalSettings);

        if (Objects.nonNull(gateway) && (!GeneralSmscConstants.SIP_PROTOCOL.equalsIgnoreCase(Objects.requireNonNull(selectedDestination).getProtocol()) || Objects.isNull(registerData))) {
            when(gateways.get(anyInt())).thenReturn(gateway);
        }

        if (Objects.nonNull(selectedDestination) && GeneralSmscConstants.DIAMETER_PROTOCOL.equalsIgnoreCase(selectedDestination.getProtocol())) {
            when(diameterGatewayMap.get(anyInt())).thenReturn(mock(DiameterConfig.class));
        }

        if (Objects.nonNull(selectedDestination) && GeneralSmscConstants.SIP_PROTOCOL.equalsIgnoreCase(selectedDestination.getProtocol())) {
            when(scyllaManager.getSipRegistrationByMsisdn(anyString())).thenReturn(registerData);
            when(routingMatcher.validateAndGetNextNonSipDestination(any(RoutingRule.Destination.class), anyList(), any(MessageEvent.class), nullable(MessageRegister.class))).thenCallRealMethod();
            if (Objects.nonNull(registerData)) {
                when(sipGatewaysConcurrentMap.get(selectedDestination.getNetworkId())).thenReturn(
                        SipGateways.builder()
                                .networkId(selectedDestination.getNetworkId())
                                .name("sipGateways")
                                .build()
                );
            }
        }

        int requestDlr = messageEvent.getRegisteredDelivery();
        String originalShortMessage = messageEvent.getShortMessage();
        commonProcessor.processMessage(messageEvent);
        this.checkMessageWithRule(messageEvent, routingRule, originalShortMessage);

        if (Objects.nonNull(gateway)) {
            // check the Request DLR values
            if (RequestDelivery.TRANSPARENT.getValue() != gateway.getRequestDLR()) {
                assertEquals(gateway.getRequestDLR(), messageEvent.getRegisteredDelivery());
            } else {
                assertEquals(requestDlr, messageEvent.getRegisteredDelivery());
            }

        }

        if (Objects.nonNull(expectedMessageBytes)) {
            assertArrayEquals(expectedMessageBytes, messageEvent.getMessageBytes());
        }
    }

    @Test
    @DisplayName("processMessage should fail when SIP destination has no registration and no fallback destination")
    void processMessageWhenSipDestinationIsNotRegisteredAndNoFallbackThenSendFailedCdr() {
        MessageEvent messageEvent = getSingleMessage();
        RoutingRule routingRule = getRuleWithActionsAndSipDestination();

        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(routingRule.getDestination().getFirst());
        when(scyllaManager.getSipRegistrationByMsisdn(anyString())).thenReturn(null);
        when(routingMatcher.validateAndGetNextNonSipDestination(any(RoutingRule.Destination.class), anyList(), any(MessageEvent.class), nullable(MessageRegister.class)))
                .thenReturn(null);

        commonProcessor.processMessage(messageEvent);

        assertEquals(ErrorCodes.NOT_DESTINATION_ADDR_REGISTERED, messageEvent.getErrorCode());

        ArgumentCaptor<String> cdrCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CDR_TOPIC), cdrCaptor.capture());

        UtilsRecords.Cdr sentCdr = Converter.stringToObject(cdrCaptor.getValue(), UtilsRecords.Cdr.class);
        assertEquals(UtilsEnum.CdrStatus.FAILED.name(), sentCdr.status());
        assertEquals(String.valueOf(ErrorCodes.NOT_DESTINATION_ADDR_REGISTERED), sentCdr.statusCode());
        assertEquals(messageEvent.getMessageId(), sentCdr.messageId());
    }

    private void checkMessageWithRule(MessageEvent messageEvent, RoutingRule routingRule, String originalShortMessage) {
        RoutingRule.Destination firstDestination = routingRule.getDestination().stream()
                .min(Comparator.comparingInt(RoutingRule.Destination::getPriority))
                .orElse(null);

        assertNotNull(firstDestination);
        assertEquals(routingRule.getId(), messageEvent.getRoutingId());

        if (GeneralSmscConstants.SIP_PROTOCOL.equalsIgnoreCase(firstDestination.getProtocol())) {
            if (messageEvent.isSipMessageSupported()) {
                assertEquals(GeneralSmscConstants.SIP_PROTOCOL, messageEvent.getDestProtocol());
            } else {
                assertFalse(GeneralSmscConstants.SIP_PROTOCOL.equalsIgnoreCase(messageEvent.getDestProtocol()));
                assertTrue(messageEvent.getRetryDestNetworkId().contains(String.valueOf(firstDestination.getNetworkId())));
            }
        } else {
            assertEquals(firstDestination.getNetworkType(), messageEvent.getDestNetworkType());
            assertEquals(firstDestination.getProtocol(), messageEvent.getDestProtocol());
            assertEquals(firstDestination.getNetworkId(), messageEvent.getDestNetworkId());
        }

        if (routingRule.isHasActionRules()) {
            //Check Source Address
            if (!routingRule.getNewSourceAddr().isEmpty()) {
                assertEquals(routingRule.getNewSourceAddr(), messageEvent.getSourceAddr());
            }
            if (routingRule.getNewSourceAddrTon() > -1) {
                assertEquals(routingRule.getNewSourceAddrTon(), messageEvent.getSourceAddrTon());
            }
            if (routingRule.getNewSourceAddrNpi() > -1) {
                assertEquals(routingRule.getNewSourceAddrNpi(), messageEvent.getSourceAddrNpi());
            }
            if (!routingRule.getRemoveSourceAddrPrefix().isEmpty()) {
                assertFalse(messageEvent.getSourceAddr().startsWith(routingRule.getRemoveSourceAddrPrefix()));
            }
            if (!routingRule.getAddSourceAddrPrefix().isEmpty()) {
                assertTrue(messageEvent.getSourceAddr().startsWith(routingRule.getAddSourceAddrPrefix()));
            }

            //Check Destination Address
            if (!routingRule.getNewDestinationAddr().isEmpty()) {
                assertEquals(routingRule.getNewDestinationAddr(), messageEvent.getDestinationAddr());
            }
            if (routingRule.getNewDestAddrTon() > -1) {
                assertEquals(routingRule.getNewDestAddrTon(), messageEvent.getDestAddrTon());
            }
            if (routingRule.getNewDestAddrNpi() > -1) {
                assertEquals(routingRule.getNewDestAddrNpi(), messageEvent.getDestAddrNpi());
            }
            if (!routingRule.getRemoveDestAddrPrefix().isEmpty()) {
                assertFalse(messageEvent.getDestinationAddr().startsWith(routingRule.getRemoveDestAddrPrefix()));
            }
            if (!routingRule.getAddDestAddrPrefix().isEmpty()) {
                assertTrue(messageEvent.getDestinationAddr().startsWith(routingRule.getAddDestAddrPrefix()));
            }

            String newShortMessage = Optional.ofNullable(routingRule.getNewShortMessage()).orElse("");
            if (!newShortMessage.isEmpty()) {
                List<String> regexList = StaticMethods.extractRegexFromMessage(newShortMessage);
                List<String> matches = StaticMethods.applyRegexToInput(originalShortMessage, regexList);
                String expectedShortMessage = StaticMethods.buildOutput(newShortMessage, matches);
                assertEquals(expectedShortMessage, messageEvent.getShortMessage());
                assertEquals(expectedShortMessage, messageEvent.getDelReceipt());
            }
        }
    }

    @Test
    @DisplayName("processMessage should replace short message by regex and set delReceipt")
    void processMessageWhenRoutingRuleReplacesShortMessageThenUpdateShortMessageAndDelReceipt() {
        MessageEvent messageEvent = getSingleMessage();
        messageEvent.setShortMessage("Code: 12345");

        RoutingRule routingRule = getRuleWithActionsAndSmppDestination();
        routingRule.setNewShortMessage("OTP __Code: (\\d+)__ confirmed");

        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(routingRule.getDestination().getFirst());
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());
        when(gateways.get(anyInt())).thenReturn(Gateway.builder()
                .name("GW")
                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                .splitSmppType("TLV")
                .build());

        String originalShortMessage = messageEvent.getShortMessage();
        commonProcessor.processMessage(messageEvent);

        this.checkMessageWithRule(messageEvent, routingRule, originalShortMessage);
    }

    @Test
    @DisplayName("processMessagePayloadTlvIfApply should clear payload fields for SMPP destination without split")
    void processMessagePayloadTlvIfApplyWhenSmppAndMessagePayloadAndNoSplitThenClearFields() {
        when(gateways.get(2)).thenReturn(Gateway.builder().networkId(2).splitMessage(false).build());
        MessageEvent messageEvent = MessageEvent.builder()
                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .destNetworkId(2)
                .shortMessage("hello")
                .messageBytes(new byte[]{1, 2, 3})
                .optionalParameters(new java.util.ArrayList<>(List.of(
                        new UtilsRecords.OptionalParameter(OptionalParameter.Tag.MESSAGE_PAYLOAD.code(), "hello")
                )))
                .build();
        messageEvent.setUdhBytes(new byte[]{1, 2});
        messageEvent.setUdhRaw(Set.of(new Udh("05", "05043E940000")));

        commonProcessor.processMessagePayloadTlvIfApply(messageEvent);

        assertEquals("", messageEvent.getShortMessage());
        assertArrayEquals(new byte[]{}, messageEvent.getMessageBytes());
        assertEquals("", messageEvent.getDelReceipt());
        assertArrayEquals(new byte[]{}, messageEvent.getUdhBytes());
        assertNotNull(messageEvent.getUdhRaw());
        assertTrue(messageEvent.getUdhRaw().isEmpty());
        assertTrue(messageEvent.getOptionalParameters().stream()
                .anyMatch(op -> op.tag() == OptionalParameter.Tag.MESSAGE_PAYLOAD.code()));
    }

    @Test
    @DisplayName("incrementCreditUsedByOriginNetworkId should group and increment per origin network id")
    void incrementCreditUsedByOriginNetworkIdShouldAggregateAndIncrementCreditHandler() {
        MessageEvent spOne = MessageEvent.builder().originNetworkType("SP").originNetworkId(1).build();
        MessageEvent spOneWithParts = MessageEvent.builder()
                .originNetworkType("SP")
                .originNetworkId(1)
                .messageParts(List.of(new com.paicbd.smsc.dto.MessagePart(), new com.paicbd.smsc.dto.MessagePart()))
                .build();
        MessageEvent spTwo = MessageEvent.builder().originNetworkType("SP").originNetworkId(2).build();
        MessageEvent gwMessage = MessageEvent.builder().originNetworkType("GW").originNetworkId(1).build();

        Runnable runnable = commonProcessor.incrementCreditUsedByOriginNetworkId(List.of(spOne, spOneWithParts, spTwo, gwMessage));
        runnable.run();

        verify(creditHandler).incrementCreditUsed(1, 3);
        verify(creditHandler).incrementCreditUsed(2, 1);
    }

    private static Stream<Arguments> provideTestCasesForProcessMessage() {
        return Stream.of(
                // Test case for destination SMPP and GW with REQUEST_DLR
                Arguments.of(
                        getSingleMessage(),
                        getRuleWithActionsAndSmppDestination(),
                        Gateway.builder()
                                .name("GW")
                                .requestDLR(RequestDelivery.REQUEST_DLR.getValue())
                                .splitSmppType("TLV")
                                .build(),
                        null,
                        null,
                        null
                ),
                // Test case for destination SMPP and GW with NON_REQUEST_DLR
                Arguments.of(
                        getSingleMessage(),
                        getRuleWithActionsAndSmppDestination(),
                        Gateway.builder()
                                .name("GW")
                                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                                .splitSmppType("TLV")
                                .build(),
                        null,
                        null,
                        null
                ),
                // Test case for destination SMPP and GW with TRANSPARENT
                Arguments.of(
                        getSingleMessage(),
                        getRuleWithActionsAndSmppDestination(),
                        Gateway.builder()
                                .name("GW")
                                .requestDLR(RequestDelivery.TRANSPARENT.getValue())
                                .splitSmppType("TLV")
                                .build(),
                        null,
                        null,
                        null
                ),
                // Test case for destination SS7
                Arguments.of(
                        getSingleMessage(),
                        getRuleWithActionsAndSs7Destination(),
                        null,
                        Ss7Settings.builder()
                                .name("ss7")
                                .protocol(GeneralSmscConstants.SS7_PROTOCOL)
                                .networkId(2)
                                .globalTitle("50588655545")
                                .globalTitleIndicator(UtilsEnum.GlobalTitleIndicator.GT0100)
                                .translationType(0)
                                .smscSsn(8)
                                .hlrSsn(6)
                                .mscSsn(8)
                                .mapVersion(3)
                                .splitMessage(true)
                                .build(),
                        null,
                        null
                ),
                // Test case for destination SIP (origin SMPP, SIP supported)
                Arguments.of(
                        getSingleMessage(),
                        getRuleWithActionsAndSipDestination(),
                        null,
                        null,
                        MessageRegister.builder()
                                .msisdn("5055849393")
                                .aor("sip:user@domain")
                                .regEventState(RegEventState.ACTIVE)
                                .build(),
                        null
                ),
                // SIP origin should keep SIP destination when destination supports SIP
                Arguments.of(
                        getSingleSipOriginMessage(),
                        getRuleWithActionsAndSipDestination(),
                        null,
                        null,
                        MessageRegister.builder()
                                .msisdn("5055849393")
                                .aor("sip:user@domain")
                                .regEventState(RegEventState.ACTIVE)
                                .build(),
                        null
                ),
                // SIP origin should fallback from SIP to SMPP when destination does not support SIP
                Arguments.of(
                        getSingleSipOriginMessage(),
                        getRuleWithActionsAndSipDestination(),
                        Gateway.builder()
                                .name("GW")
                                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                                .splitSmppType("TLV")
                                .build(),
                        null,
                        null,
                        null
                ),
                // SIP origin should route directly to SMPP destination
                Arguments.of(
                        getSingleSipOriginMessage(),
                        getRuleWithActionsAndSmppDestination(),
                        Gateway.builder()
                                .name("GW")
                                .requestDLR(RequestDelivery.REQUEST_DLR.getValue())
                                .splitSmppType("TLV")
                                .build(),
                        null,
                        null,
                        null
                ),
                // SIP origin should route directly to SS7 destination
                Arguments.of(
                        getSingleSipOriginMessage(),
                        getRuleWithActionsAndSs7Destination(),
                        null,
                        Ss7Settings.builder()
                                .name("ss7")
                                .protocol(GeneralSmscConstants.SS7_PROTOCOL)
                                .networkId(2)
                                .globalTitle("50588655545")
                                .globalTitleIndicator(UtilsEnum.GlobalTitleIndicator.GT0100)
                                .translationType(0)
                                .smscSsn(8)
                                .hlrSsn(6)
                                .mscSsn(8)
                                .mapVersion(3)
                                .splitMessage(true)
                                .build(),
                        null,
                        null
                ),
                // SIP origin should fallback from SIP to SS7 when destination does not support SIP
                Arguments.of(
                        getSingleSipOriginMessage(),
                        getRuleWithActionsAndSipAndSs7Destination(),
                        null,
                        Ss7Settings.builder()
                                .name("ss7")
                                .protocol(GeneralSmscConstants.SS7_PROTOCOL)
                                .networkId(3)
                                .globalTitle("50588655545")
                                .globalTitleIndicator(UtilsEnum.GlobalTitleIndicator.GT0100)
                                .translationType(0)
                                .smscSsn(8)
                                .hlrSsn(6)
                                .mscSsn(8)
                                .mapVersion(3)
                                .splitMessage(true)
                                .build(),
                        null,
                        null
                ),
                // Test case for destination DIAMETER
                Arguments.of(
                        getSingleMessage(),
                        getRuleWithActionsAndDiameterDestination(),
                        null,
                        null,
                        null,
                        null
                ),
                Arguments.of(
                        buildBroadcastHexMessage(EncodingUtils.GSM7, 0),
                        getRuleWithActionsAndSmppDestination(),
                        Gateway.builder()
                                .name("GW")
                                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                                .splitSmppType("TLV")
                                .build(),
                        null,
                        null,
                        EncodingUtils.encodeMessage("Hello", EncodingUtils.GSM7)
                ),
                Arguments.of(
                        buildBroadcastHexMessage(EncodingUtils.UCS2, 8),
                        getRuleWithActionsAndSmppDestination(),
                        Gateway.builder()
                                .name("GW")
                                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                                .splitSmppType("TLV")
                                .build(),
                        null,
                        null,
                        EncodingUtils.encodeMessage("Hello", EncodingUtils.UCS2)
                ),
                Arguments.of(
                        buildBroadcastPlainTextMessage(0),
                        getRuleWithActionsAndSmppDestination(),
                        Gateway.builder()
                                .name("GW")
                                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                                .splitSmppType("TLV")
                                .build(),
                        null,
                        null,
                        EncodingUtils.encodeMessage("Hello", EncodingUtils.GSM7)
                ),
                Arguments.of(
                        buildBroadcastPlainTextMessage(8),
                        getRuleWithActionsAndSmppDestination(),
                        Gateway.builder()
                                .name("GW")
                                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                                .splitSmppType("TLV")
                                .build(),
                        null,
                        null,
                        EncodingUtils.encodeMessage("Hello", EncodingUtils.UCS2)
                )
        );
    }

    public static MessageEvent getSingleMessage() {
        return MessageEvent.builder()
                .id("1722442489766-778860479922")
                .parentId("1722442489766-7788604799226")
                .messageId("1722442489766-7788604799226")
                .systemId("smpp_sp")
                .commandStatus(0)
                .segmentSequence(2)
                .sourceAddr("50588888888")
                .destinationAddr("50599999999")
                .validityPeriod(60)
                .registeredDelivery(RequestDelivery.NON_REQUEST_DLR.getValue())
                .retryDestNetworkId("")
                .dataCoding(0)
                .shortMessage("Hello I'm message with destination SMPP")
                .originNetworkId(1)
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .destAddrNpi(1)
                .destAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddrTon(1)
                .esmClass(3)
                .build();
    }

    public static MessageEvent getSingleSipOriginMessage() {
        MessageEvent messageEvent = getSingleMessage();
        messageEvent.setOriginProtocol(GeneralSmscConstants.SIP_PROTOCOL);
        messageEvent.setSystemId("sip_sp");
        return messageEvent;
    }

    public static RoutingRule getRuleWithActionsAndSmppDestination() {
        RoutingRule.Destination destinationGwSmpp = new RoutingRule.Destination();
        destinationGwSmpp.setPriority(1);
        destinationGwSmpp.setNetworkId(2);
        destinationGwSmpp.setProtocol(GeneralSmscConstants.SMPP_PROTOCOL);
        destinationGwSmpp.setNetworkType("GW");
        return RoutingRule.builder()
                .id(11)
                .originNetworkId(1)
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .newSourceAddr("")
                .newSourceAddrTon(2)
                .newSourceAddrNpi(2)
                .newDestinationAddr("")
                .addSourceAddrPrefix("")
                .removeSourceAddrPrefix("")
                .newDestAddrTon(2)
                .newDestAddrNpi(2)
                .hasActionRules(true)
                .addDestAddrPrefix("")
                .removeDestAddrPrefix("")
                .newGtSccpAddr("")
                .autoMapVersion(true)
                .hasActionAdvancedRules(false)
                .destination(List.of(destinationGwSmpp))
                .autoMapVersion(true)
                .hasActionAdvancedRules(false)
                .build();
    }

    public static RoutingRule getRuleWithActionsAndSipDestination() {
        RoutingRule.Destination destinationGwSip = new RoutingRule.Destination();
        destinationGwSip.setPriority(1);
        destinationGwSip.setNetworkId(2);
        destinationGwSip.setProtocol(GeneralSmscConstants.SIP_PROTOCOL);
        destinationGwSip.setNetworkType("GW");

        RoutingRule.Destination destinationGwSmpp = new RoutingRule.Destination();
        destinationGwSmpp.setPriority(2);
        destinationGwSmpp.setNetworkId(3);
        destinationGwSmpp.setProtocol(GeneralSmscConstants.SMPP_PROTOCOL);
        destinationGwSmpp.setNetworkType("GW");

        return RoutingRule.builder()
                .id(11)
                .originNetworkId(1)
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .newSourceAddr("")
                .newSourceAddrTon(2)
                .newSourceAddrNpi(2)
                .newDestinationAddr("")
                .addSourceAddrPrefix("")
                .removeSourceAddrPrefix("")
                .newDestAddrTon(2)
                .newDestAddrNpi(2)
                .hasActionRules(true)
                .addDestAddrPrefix("")
                .removeDestAddrPrefix("")
                .newGtSccpAddr("")
                .autoMapVersion(true)
                .hasActionAdvancedRules(false)
                .destination(List.of(destinationGwSip, destinationGwSmpp))
                .autoMapVersion(true)
                .hasActionAdvancedRules(false)
                .build();
    }

    public static RoutingRule getRuleWithActionsAndSs7Destination() {
        RoutingRule.Destination destinationGwSmpp = new RoutingRule.Destination();
        destinationGwSmpp.setPriority(1);
        destinationGwSmpp.setNetworkId(2);
        destinationGwSmpp.setProtocol(GeneralSmscConstants.SS7_PROTOCOL);
        destinationGwSmpp.setNetworkType("GW");
        RoutingRule.ActionAdvanced actionAdvanced = new RoutingRule.ActionAdvanced();
        actionAdvanced.setMapVersion(2);
        actionAdvanced.setPriorityFlagSri(false);
        actionAdvanced.setOperationCodeMt(44);
        actionAdvanced.setOperationCodeSri(22);
        actionAdvanced.setSsnSmscMt(6);
        actionAdvanced.setSsnHlrSri(6);
        actionAdvanced.setSsnMscMt(8);
        actionAdvanced.setSsnSmscSri(8);

        return RoutingRule.builder()
                .id(11)
                .originNetworkId(1)
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .newSourceAddr("")
                .newSourceAddrTon(2)
                .newSourceAddrNpi(2)
                .newDestinationAddr("")
                .addSourceAddrPrefix("")
                .removeSourceAddrPrefix("")
                .newDestAddrTon(2)
                .newDestAddrNpi(2)
                .hasActionRules(true)
                .addDestAddrPrefix("")
                .removeDestAddrPrefix("")
                .newGtSccpAddr("{{destinationAddr}}")
                .autoMapVersion(false)
                .hasActionAdvancedRules(true)
                .destination(List.of(destinationGwSmpp))
                .actionAdvanced(actionAdvanced)
                .build();
    }

    public static RoutingRule getRuleWithActionsAndSipAndSs7Destination() {
        RoutingRule.Destination destinationGwSip = new RoutingRule.Destination();
        destinationGwSip.setPriority(1);
        destinationGwSip.setNetworkId(2);
        destinationGwSip.setProtocol(GeneralSmscConstants.SIP_PROTOCOL);
        destinationGwSip.setNetworkType("GW");

        RoutingRule.Destination destinationGwSs7 = new RoutingRule.Destination();
        destinationGwSs7.setPriority(2);
        destinationGwSs7.setNetworkId(3);
        destinationGwSs7.setProtocol(GeneralSmscConstants.SS7_PROTOCOL);
        destinationGwSs7.setNetworkType("GW");

        return RoutingRule.builder()
                .id(11)
                .originNetworkId(1)
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .newSourceAddr("")
                .newSourceAddrTon(2)
                .newSourceAddrNpi(2)
                .newDestinationAddr("")
                .addSourceAddrPrefix("")
                .removeSourceAddrPrefix("")
                .newDestAddrTon(2)
                .newDestAddrNpi(2)
                .hasActionRules(true)
                .addDestAddrPrefix("")
                .removeDestAddrPrefix("")
                .newGtSccpAddr("")
                .autoMapVersion(true)
                .hasActionAdvancedRules(false)
                .destination(List.of(destinationGwSip, destinationGwSs7))
                .build();
    }

    public static RoutingRule getRuleWithActionsAndDiameterDestination() {
        RoutingRule.Destination destinationGwDiameter = new RoutingRule.Destination();
        destinationGwDiameter.setPriority(1);
        destinationGwDiameter.setNetworkId(4);
        destinationGwDiameter.setProtocol(GeneralSmscConstants.DIAMETER_PROTOCOL);
        destinationGwDiameter.setNetworkType("GW");
        return RoutingRule.builder()
                .id(11)
                .originNetworkId(1)
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .newSourceAddr("")
                .newSourceAddrTon(2)
                .newSourceAddrNpi(2)
                .newDestinationAddr("")
                .addSourceAddrPrefix("")
                .removeSourceAddrPrefix("")
                .newDestAddrTon(2)
                .newDestAddrNpi(2)
                .hasActionRules(true)
                .addDestAddrPrefix("")
                .removeDestAddrPrefix("")
                .newGtSccpAddr("")
                .autoMapVersion(true)
                .hasActionAdvancedRules(false)
                .destination(List.of(destinationGwDiameter))
                .build();
    }

    private static MessageEvent buildBroadcastHexMessage(int encoding, int dataCoding) {
        byte [] bytes = EncodingUtils.encodeMessage("Hello", encoding);
        MessageEvent messageEvent = getSingleMessage();
        messageEvent.setBroadcastId(1);
        messageEvent.setShortMessage(EncodingUtils.bytesToHex(bytes));
        messageEvent.setMessageBytes(bytes);
        messageEvent.setDataCoding(dataCoding);
        return messageEvent;
    }

    private static MessageEvent buildBroadcastPlainTextMessage(int dataCoding) {
        MessageEvent messageEvent = getSingleMessage();
        messageEvent.setBroadcastId(1);
        messageEvent.setShortMessage("Hello");
        messageEvent.setDataCoding(dataCoding);
        messageEvent.setMessageBytes(null);
        return messageEvent;
    }

    static String longMessage = """
            Java is a powerful, object-oriented programming language widely used
            for building enterprise applications, web services, and mobile apps.
            With features like platform independence, strong memory management,
            and multithreading capabilities, Java remains a top choice for developers.
            It supports frameworks like Spring Boot, Hibernate, and Jakarta EE,
            enabling efficient backend development. Additionally, Java's rich ecosystem,
            including tools like Maven and Gradle, simplifies project management.
            The language's compatibility with modern cloud technologies and microservices
            architecture makes it essential for scalable software solutions.
            Whether for Android development, big data, or distributed systems,
            Java continues to be highly relevant in the tech industry
            """;

    @ParameterizedTest
    @MethodSource("provideTestCasesForGetPartsOfMessageWithDifferentDestination")
    @DisplayName("Test getPartsOfMessage with SMPP destination")
    void getPartsOfMessageWithDifferentDestination(
            String message, int dataCoding, boolean splitMessage, String splitSmppType, String splitMsgReferenceType,
            String destinationProtocol, CommonProcessor.MessagePart expectedResult) {
        when(appProperties.getSplitMsgReferenceType()).thenReturn(splitMsgReferenceType);

        switch (destinationProtocol) {
            case GeneralSmscConstants.SMPP_PROTOCOL -> {
                Gateway gateway = Gateway.builder()
                        .networkId(4)
                        .name("GW")
                        .splitMessage(splitMessage)
                        .splitSmppType(splitSmppType)
                        .encodingGsm7(EncodingUtils.GSM7)
                        .encodingUcs2(EncodingUtils.UCS2)
                        .encodingIso88591(EncodingUtils.ISO88591)
                        .build();

                when(gateways.get(4)).thenReturn(gateway);
            }

            case GeneralSmscConstants.SS7_PROTOCOL -> {
                Ss7Settings ss7Config = Ss7Settings.builder()
                        .networkId(4)
                        .globalTitle("5058888888")
                        .globalTitleIndicator(UtilsEnum.GlobalTitleIndicator.GT0100)
                        .translationType(1)
                        .smscSsn(8)
                        .hlrSsn(6)
                        .mscSsn(8)
                        .mapVersion(3)
                        .splitMessage(true)
                        .build();
                when(settingsLoader.getSs7Settings(4)).thenReturn(ss7Config);
            }

            case GeneralSmscConstants.DIAMETER_PROTOCOL -> {
                DiameterConfig diameterConfig = new DiameterConfig();
                diameterConfig.setId(1);
                diameterConfig.setNetworkId(4);
                diameterConfig.setName("diameterConfig");
                diameterConfig.setEnabled(true);
                diameterConfig.setSplitMessage(true);
                when(diameterGatewayMap.get(anyInt())).thenReturn(diameterConfig);
            }

            default -> throw new IllegalStateException("Unexpected value: " + destinationProtocol);
        }

        MessageEvent messageEvent = MessageEvent.builder()
                .id("1722442489766-7788604799226")
                .messageId("1722442489770-7788608933795")
                .systemId("http_sp")
                .commandStatus(0)
                .segmentSequence(2)
                .sourceAddr("50588888888")
                .destinationAddr("50599999999")
                .validityPeriod(0)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                .originNetworkId(1)
                .destNetworkType("GW")
                .destProtocol(destinationProtocol)
                .destNetworkId(4)
                .build();

        messageEvent.setShortMessage(message);
        messageEvent.setDataCoding(dataCoding);

        commonProcessor = new CommonProcessor(creditHandler, settingsLoader,
                routingMatcher, gateways, serviceProviders, scyllaManager, kafkaTemplate, diameterGatewayMap, appProperties, sipGatewaysConcurrentMap);
        CommonProcessor.MessagePart result = commonProcessor.getPartsOfMessage(messageEvent);
        assertNotNull(result);
        assertEquals(expectedResult, result);
    }

    private static Stream<Arguments> provideTestCasesForGetPartsOfMessageWithDifferentDestination() {
        return Stream.of(
                // SS7, GSM7 + UDH + 8BIT → 153 chars/seg, 5 parts
                Arguments.of(longMessage, 0, true, "UDH", "8BIT", GeneralSmscConstants.SS7_PROTOCOL,
                        new CommonProcessor.MessagePart(5, 153, 0, true)),

                // SS7, GSM7 + UDH + 16BIT → 152 chars/seg, 6 parts
                Arguments.of(longMessage, 0, true, "UDH", "16BIT", GeneralSmscConstants.SS7_PROTOCOL,
                        new CommonProcessor.MessagePart(6, 152, 0, true)),

                // SS7, UCS2 + UDH + 8BIT → 67 chars/seg, 12 parts
                Arguments.of(longMessage, 8, true, "UDH", "8BIT", GeneralSmscConstants.SS7_PROTOCOL,
                        new CommonProcessor.MessagePart(12, 67, 8, true)),
                // SS7, UCS2 + UDH + 16BIT → 66 chars/seg, 12 parts
                Arguments.of(longMessage, 8, true, "UDH", "16BIT", GeneralSmscConstants.SS7_PROTOCOL,
                        new CommonProcessor.MessagePart(12, 66, 8, true)),

                // Diameter, GSM7 + UDH + 8BIT → 153 chars/seg, 5 parts
                Arguments.of(longMessage, 0, true, "UDH", "8BIT", GeneralSmscConstants.DIAMETER_PROTOCOL,
                        new CommonProcessor.MessagePart(5, 153, 0, true)),

                // Diameter, GSM7 + UDH + 16BIT → 152 chars/seg, 6 parts
                Arguments.of(longMessage, 0, true, "UDH", "16BIT", GeneralSmscConstants.DIAMETER_PROTOCOL,
                        new CommonProcessor.MessagePart(6, 152, 0, true)),

                // Diameter, UCS2 + UDH + 8BIT → 67 chars/seg, 12 parts
                Arguments.of(longMessage, 8, true, "UDH", "8BIT", GeneralSmscConstants.DIAMETER_PROTOCOL,
                        new CommonProcessor.MessagePart(12, 67, 8, true)),
                // Diameter, UCS2 + UDH + 16BIT → 66 chars/seg, 12 parts
                Arguments.of(longMessage, 8, true, "UDH", "16BIT", GeneralSmscConstants.DIAMETER_PROTOCOL,
                        new CommonProcessor.MessagePart(12, 66, 8, true)),

                // GSM7 + UDH + 8BIT → 153 chars/seg, 5 parts
                Arguments.of(longMessage, 0, true, "UDH", "8BIT", GeneralSmscConstants.SMPP_PROTOCOL,
                        new CommonProcessor.MessagePart(5, 153, 0, true)),
                // GSM7 + UDH + 16BIT → 152 chars/seg, 6 parts
                Arguments.of(longMessage, 0, true, "UDH", "16BIT", GeneralSmscConstants.SMPP_PROTOCOL,
                        new CommonProcessor.MessagePart(6, 152, 0, true)),
                // GSM7 + TLV + 8BIT → 153 chars/seg, 5 parts
                Arguments.of(longMessage, 0, true, "TLV", "8BIT", GeneralSmscConstants.SMPP_PROTOCOL,
                        new CommonProcessor.MessagePart(5, 153, 0, false)),
                // GSM7 + TLV + 16BIT → 152 chars/seg, 6 parts
                Arguments.of(longMessage, 0, true, "TLV", "16BIT", GeneralSmscConstants.SMPP_PROTOCOL,
                        new CommonProcessor.MessagePart(6, 152, 0, false)),
                // UCS2 + UDH + 8BIT → 67 chars/seg, 12 parts
                Arguments.of(longMessage, 8, true, "UDH", "8BIT", GeneralSmscConstants.SMPP_PROTOCOL,
                        new CommonProcessor.MessagePart(12, 67, 8, true)),
                // UCS2 + UDH + 16BIT → 66 chars/seg, 12 parts
                Arguments.of(longMessage, 8, true, "UDH", "16BIT", GeneralSmscConstants.SMPP_PROTOCOL,
                        new CommonProcessor.MessagePart(12, 66, 8, true)),
                // UCS2 + TLV + 8BIT → 67 chars/seg, 12 parts
                Arguments.of(longMessage, 8, true, "TLV", "8BIT", GeneralSmscConstants.SMPP_PROTOCOL,
                        new CommonProcessor.MessagePart(12, 67, 8, false)),
                // UCS2 + TLV + 16BIT → 66 chars/seg, 12 parts
                Arguments.of(longMessage, 8, true, "TLV", "16BIT", GeneralSmscConstants.SMPP_PROTOCOL,
                        new CommonProcessor.MessagePart(12, 66, 8, false))
        );
    }

    @ParameterizedTest
    @MethodSource("provideTestCasesForProcessDlr")
    void processDlrWithDestinationSmpp(MessageEvent messageEvent, UtilsRecords.SubmitSmResponseEvent submitSmResponseEvent, boolean useGeneralSetting, boolean useSS7Settings) {
        when(scyllaManager.selectFromTable(anyString(), anyString())).thenReturn(submitSmResponseEvent.toString());

        String messagePriorityOfDlr = (String) submitSmResponseEvent.customParams().get(GeneralSmscConstants.MESSAGE_PRIORITY);
        if (Objects.isNull(messagePriorityOfDlr)) {
            messagePriorityOfDlr = GeneralSmscConstants.MEDIUM_PRIORITY;
        }

        if (useGeneralSetting) {
            GeneralSettings generalSettings = GeneralSettings.builder()
                    .validityPeriod(120)
                    .maxValidityPeriod(240)
                    .id(1)
                    .destAddrNpi(1)
                    .destAddrTon(1)
                    .sourceAddrNpi(1)
                    .sourceAddrTon(1)
                    .encodingGsm7(0)
                    .encodingUcs2(2)
                    .encodingIso88591(3)
                    .build();

            when(settingsLoader.getSmppHttpSettings()).thenReturn(generalSettings);
        }

        if (useSS7Settings) {
            Ss7Settings ss7Config = Ss7Settings.builder()
                    .globalTitle("50588655545")
                    .globalTitleIndicator(UtilsEnum.GlobalTitleIndicator.GT0100)
                    .translationType(1)
                    .smscSsn(8)
                    .hlrSsn(6)
                    .mscSsn(8)
                    .mapVersion(2)
                    .build();

            when(settingsLoader.getSs7Settings(anyInt())).thenReturn(ss7Config);
        }

        commonProcessor = new CommonProcessor(creditHandler, settingsLoader,
                routingMatcher, gateways, serviceProviders, scyllaManager, kafkaTemplate, diameterGatewayMap, appProperties, sipGatewaysConcurrentMap);
        assertDoesNotThrow(() -> commonProcessor.processDlr(messageEvent));

        if (useGeneralSetting) {
            ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> eventSentToKafka = ArgumentCaptor.forClass(String.class);

            Awaitility
                    .await()
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> verify(kafkaTemplate, atLeastOnce()).send(
                            topicCaptor.capture(),
                            eventSentToKafka.capture()));

            assertNotNull(eventSentToKafka.getValue());
            MessageEvent eventResult = Converter.stringToObject(eventSentToKafka.getValue(), MessageEvent.class);
            assertEquals(eventResult.getDestNetworkId(), submitSmResponseEvent.originNetworkId());
            assertEquals(eventResult.getDestProtocol(), submitSmResponseEvent.originProtocol());

            if (useSS7Settings) {
                assertEquals(8, eventResult.getSmscSsn());
                assertEquals(8, eventResult.getMscSsn());
                assertEquals(6, eventResult.getHlrSsn());
                assertEquals(2, eventResult.getMapVersion());
            }
            assertNotNull(eventResult.getSmscMessagePriority());
            String currentPriority = eventResult.getSmscMessagePriority();
            assertEquals(messagePriorityOfDlr, currentPriority);
        }

        if (messageEvent.getMessageId().equalsIgnoreCase("1722442489770-7788608933794")) {
            verify(scyllaManager).deleteFromTable(anyString(), anyString());
        }
    }

    private static Stream<Arguments> provideTestCasesForProcessDlr() {
        HashMap<String, Object> highPriorityHashMap = new HashMap<>();
        highPriorityHashMap.put(GeneralSmscConstants.MESSAGE_PRIORITY, GeneralSmscConstants.HIGH_PRIORITY);
        HashMap<String, Object> mediumPriorityHashMap = new HashMap<>();
        mediumPriorityHashMap.put(GeneralSmscConstants.MESSAGE_PRIORITY, GeneralSmscConstants.MEDIUM_PRIORITY);
        HashMap<String, Object> lowPriorityHashMap = new HashMap<>();
        lowPriorityHashMap.put(GeneralSmscConstants.MESSAGE_PRIORITY, GeneralSmscConstants.LOW_PRIORITY);
        return Stream.of(
                // Test case for destination SMPP
                Arguments.of(
                        MessageEvent.builder()
                                .id("1722442489766-7788604799226")
                                .messageId("1722442489770-7788608933795")
                                .deliverSmId("1722442489766-7788604799227")
                                .systemId("smpp_sp")
                                .commandStatus(0)
                                .segmentSequence(2)
                                .sourceAddr("50588888888")
                                .destinationAddr("50599999999")
                                .validityPeriod(0)
                                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                                .status("DELIVRD")
                                .originNetworkType("SP")
                                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .originNetworkId(1)
                                .destNetworkType("GW")
                                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .destNetworkId(4)
                                .dataCoding(0)
                                .checkSubmitSmResponse(true)
                                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                                .build(),
                        new UtilsRecords.SubmitSmResponseEvent(
                                "",
                                "1722442489766-7788604799226",
                                "smpp_sp",
                                "1722442489766-7788604799227",
                                "1722442489766-7788604799226",
                                GeneralSmscConstants.SMPP_PROTOCOL,
                                1,
                                "SP",
                                "",
                                0,
                                0,
                                "1722442489766-7788604799226",
                                2,
                                false,
                                highPriorityHashMap,
                                false,
                                "",
                                "",
                                GeneralSmscConstants.HIGH_PRIORITY
                        ),
                        true,
                        false
                ),
                Arguments.of(
                        MessageEvent.builder()
                                .id("1722442489766-7788604799223")
                                .messageId("1722442489770-7788608933794")
                                .deliverSmId("1722442489766-7788604799225")
                                .systemId("smpp_sp")
                                .commandStatus(0)
                                .segmentSequence(2)
                                .sourceAddr("50588888888")
                                .destinationAddr("50599999999")
                                .validityPeriod(0)
                                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                                .status("DELIVRD")
                                .originNetworkType("SP")
                                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .originNetworkId(1)
                                .destNetworkType("GW")
                                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .destNetworkId(4)
                                .dataCoding(0)
                                .checkSubmitSmResponse(true)
                                .process(true)
                                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                                .build(),
                        new UtilsRecords.SubmitSmResponseEvent(
                                "",
                                "1722442489766-7788604799223",
                                "smpp_sp",
                                "1722442489766-7788604799225",
                                "1722442489766-7788604799223",
                                GeneralSmscConstants.SMPP_PROTOCOL,
                                1,
                                "SP",
                                "",
                                0,
                                0,
                                "1722442489766-7788604799223",
                                2,
                                false,
                                mediumPriorityHashMap,
                                false,
                                "",
                                "",
                                GeneralSmscConstants.MEDIUM_PRIORITY
                        ),
                        true,
                        false
                ),
                Arguments.of(
                        MessageEvent.builder()
                                .id("1722442489766-7788604799213")
                                .messageId("1722442489770-7788608933714")
                                .deliverSmId("1722442489766-7788604799215")
                                .systemId("smpp_sp")
                                .commandStatus(0)
                                .segmentSequence(2)
                                .sourceAddr("50588888888")
                                .destinationAddr("50599999999")
                                .validityPeriod(0)
                                .smscMessagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                                .status("DELIVRD")
                                .originNetworkType("SP")
                                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .originNetworkId(1)
                                .destNetworkType("GW")
                                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .destNetworkId(4)
                                .dataCoding(0)
                                .checkSubmitSmResponse(true)
                                .process(true)
                                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                                .build(),
                        new UtilsRecords.SubmitSmResponseEvent(
                                "",
                                "1722442489766-7788604799213",
                                "smpp_sp",
                                "1722442489766-7788604799215",
                                "1722442489766-7788604799213",
                                GeneralSmscConstants.SMPP_PROTOCOL,
                                1,
                                "GW",
                                "",
                                0,
                                0,
                                "1722442489766-7788604799213",
                                2,
                                false,
                               lowPriorityHashMap,
                                false,
                                "",
                                "",
                                GeneralSmscConstants.MEDIUM_PRIORITY
                        ),
                        false,
                        false
                ),
                Arguments.of(
                        MessageEvent.builder()
                                .id("1722442489766-7788604799233")
                                .messageId("1722442489770-7788608933734")
                                .deliverSmId("1722442489766-7788604799235")
                                .systemId("smpp_sp")
                                .commandStatus(0)
                                .segmentSequence(2)
                                .sourceAddr("50588888888")
                                .destinationAddr("50599999999")
                                .validityPeriod(0)
                                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                                .delReceipt("69643A31207375623A30303120646C7672643A303031207375626D697420646174653A3231303130313030303020646F6E6520646174653A3231303130313030303020737461743A44454C49565244206572723A30303020746578743A54657374204D657373616765")
                                .status("DELIVRD")
                                .originNetworkType("SP")
                                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .originNetworkId(1)
                                .destNetworkType("GW")
                                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .destNetworkId(4)
                                .dataCoding(0)
                                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                                .checkSubmitSmResponse(true)
                                .process(true)
                                .build(),
                        new UtilsRecords.SubmitSmResponseEvent(
                                "",
                                "1722442489766-7788604799233",
                                "smpp_sp",
                                "1722442489766-7788604799235",
                                "1722442489766-7788604799233",
                                GeneralSmscConstants.SS7_PROTOCOL,
                                1,
                                "GW",
                                "",
                                0,
                                0,
                                "1722442489766-7788604799233",
                                2,
                                false,
                                new HashMap<>(),
                                false,
                                "",
                                "",
                                GeneralSmscConstants.MEDIUM_PRIORITY
                        ),
                        true,
                        true
                )
        );
    }

    @Test
    @DisplayName("processDlr should execute HTTP table flow when SMPP result is not found")
    void processDlrWhenMessageWasNotSentOverSmppThenUseHttpFlow() {
        String eventId = System.currentTimeMillis() + "-" + System.nanoTime();
        String messageId = System.currentTimeMillis() + "-" + System.nanoTime();
        MessageEvent messageEvent = MessageEvent.builder()
                .id(eventId)
                .messageId(messageId)
                .deliverSmId(messageId)
                .systemId("http_sp")
                .originNetworkType("GW")
                .originProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                .originNetworkId(1)
                .destNetworkType("SP")
                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .destNetworkId(4)
                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .status("DELIVRD")
                .checkSubmitSmResponse(true)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();

        UtilsRecords.SubmitSmResponseEvent submitSmResponseEvent = new UtilsRecords.SubmitSmResponseEvent(
                "",
                "parent-http-flow-1",
                "http_sp",
                "submit-sm-server-http-1",
                "parent-http-flow-1",
                GeneralSmscConstants.HTTP_PROTOCOL,
                7,
                "SP",
                "",
                0,
                0,
                "parent-http-flow-1",
                1,
                false,
                new HashMap<>(),
                false,
                "",
                "",
                GeneralSmscConstants.MEDIUM_PRIORITY
        );

        when(scyllaManager.selectFromTable(anyString(), anyString())).thenReturn(submitSmResponseEvent.toString());

        assertDoesNotThrow(() -> commonProcessor.processDlr(messageEvent));

        verify(scyllaManager).deleteFromTable(anyString(), anyString());
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
        assertNotNull(messageEvent.getDeliverSmServerId());
        assertEquals("http_sp", messageEvent.getSystemId());
        assertEquals(RequestDelivery.NON_REQUEST_DLR.getValue(), messageEvent.getRegisteredDelivery());
        assertTrue(messageEvent.isDlr());
    }

    @Test
    @DisplayName("processDlr should return when submit-sm result is null")
    void processDlrWhenResultInRawIsNullThenReturnWithoutPublishing() {
        String eventId = System.currentTimeMillis() + "-" + System.nanoTime();
        String messageId = System.currentTimeMillis() + "-" + System.nanoTime();
        MessageEvent messageEvent = MessageEvent.builder()
                .id(eventId)
                .messageId(messageId)
                .deliverSmId(messageId)
                .systemId("smpp_sp")
                .originNetworkType("GW")
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .originNetworkId(1)
                .destNetworkType("SP")
                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .destNetworkId(4)
                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .status("DELIVRD")
                .checkSubmitSmResponse(true)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();

        when(scyllaManager.selectFromTable(anyString(), anyString())).thenReturn(null);

        assertDoesNotThrow(() -> commonProcessor.processDlr(messageEvent));

        verify(scyllaManager, never()).deleteFromTable(anyString(), anyString());
        verifyNoInteractions(kafkaTemplate);
    }

    @ParameterizedTest
    @MethodSource("provideTestCasesForPublishDlrInKafkaTopicThroughProcessDlr")
    @DisplayName("processDlr should route to publishDlrInKafkaTopic when origin protocol is not HTTP/SMPP")
    void processDlrWhenOriginProtocolIsNotHttpOrSmppThenPublishByDestination(MessageEvent messageEvent, Ss7Settings ss7Settings, DiameterConfig diameterConfig,
                                                                             String expectedTopic, boolean expectedDlr, String expectedGlobalTitle) {
        if (Objects.nonNull(ss7Settings)) {
            when(settingsLoader.getSs7Settings(messageEvent.getDestNetworkId())).thenReturn(ss7Settings);
        }
        if (Objects.nonNull(diameterConfig)) {
            when(diameterGatewayMap.get(messageEvent.getDestNetworkId())).thenReturn(diameterConfig);
        }

        commonProcessor.processDlr(messageEvent);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> eventSentToKafka = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, atLeastOnce()).send(topicCaptor.capture(), eventSentToKafka.capture());

        if (Objects.nonNull(expectedTopic)) {
            assertEquals(expectedTopic, topicCaptor.getValue());
        }

        MessageEvent eventResult = Converter.stringToObject(eventSentToKafka.getValue(), MessageEvent.class);
        assertEquals(expectedDlr, eventResult.isDlr());

        if (Objects.nonNull(expectedGlobalTitle)) {
            assertEquals(expectedGlobalTitle, eventResult.getGlobalTitle());
        }
    }

    private static Stream<Arguments> provideTestCasesForPublishDlrInKafkaTopicThroughProcessDlr() {
        return Stream.of(
                Arguments.of(
                        buildDlrToPublish(GeneralSmscConstants.SIP_PROTOCOL, GeneralSmscConstants.HTTP_PROTOCOL, "SP", 1),
                        null,
                        null,
                        KafkaTopicsConstants.HTTP_DLR_TOPIC,
                        true,
                        null
                ),
                Arguments.of(
                        buildDlrToPublish(GeneralSmscConstants.SIP_PROTOCOL, GeneralSmscConstants.SMPP_PROTOCOL, "SP", 1),
                        null,
                        null,
                        KafkaTopicsConstants.SMPP_DLR_TOPIC,
                        true,
                        null
                ),
                Arguments.of(
                        buildDlrToPublish(GeneralSmscConstants.SIP_PROTOCOL, GeneralSmscConstants.SMPP_PROTOCOL, "GW", 2),
                        null,
                        null,
                        null,
                        false,
                        null
                ),
                Arguments.of(
                        buildDlrToPublish(GeneralSmscConstants.SIP_PROTOCOL, GeneralSmscConstants.SS7_PROTOCOL, "GW", 3),
                        Ss7Settings.builder()
                                .networkId(3)
                                .globalTitle("50588655545")
                                .globalTitleIndicator(UtilsEnum.GlobalTitleIndicator.GT0100)
                                .translationType(1)
                                .smscSsn(8)
                                .hlrSsn(6)
                                .mscSsn(8)
                                .mapVersion(3)
                                .build(),
                        null,
                        null,
                        true,
                        "50588655545"
                ),
                Arguments.of(
                        buildDlrToPublish(GeneralSmscConstants.SIP_PROTOCOL, GeneralSmscConstants.SIP_PROTOCOL, "GW", 4),
                        null,
                        null,
                        null,
                        true,
                        null
                ),
                Arguments.of(
                        buildDlrToPublish(GeneralSmscConstants.SIP_PROTOCOL, GeneralSmscConstants.DIAMETER_PROTOCOL, "GW", 5),
                        null,
                        DiameterConfig.builder()
                                .networkId(5)
                                .globalTitle("50588776655")
                                .build(),
                        null,
                        true,
                        "50588776655"
                ),
                Arguments.of(
                        buildDlrToPublish(GeneralSmscConstants.SS7_PROTOCOL, GeneralSmscConstants.DIAMETER_PROTOCOL, "GW", 5),
                        null,
                        DiameterConfig.builder()
                                .networkId(5)
                                .globalTitle("50588776655")
                                .build(),
                        null,
                        true,
                        "50588776655"
                )
        );
    }

    private static MessageEvent buildDlrToPublish(String originProtocol, String destProtocol, String destNetworkType, int destNetworkId) {
        String eventId = System.currentTimeMillis() + "-" + System.nanoTime();
        String messageId = System.currentTimeMillis() + "-" + System.nanoTime();
        return MessageEvent.builder()
                .id(eventId)
                .messageId(messageId)
                .deliverSmId(messageId)
                .originProtocol(originProtocol)
                .originNetworkType("SP")
                .originNetworkId(1)
                .destProtocol(destProtocol)
                .destNetworkType(destNetworkType)
                .destNetworkId(destNetworkId)
                .destinationAddr("50599999999")
                .sourceAddr("50588888888")
                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .status("DELIVRD")
                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();
    }

    @ParameterizedTest
    @DisplayName("Test getPartsOfMessage for different message events")
    @MethodSource("messageEventsForCalculateMessageParts")
    void partOfMessagesTests(MessageEvent event, int expectedParts, int lengthOfPart) {
        when(settingsLoader.getSs7Settings(anyInt())).thenReturn(Ss7Settings.builder().splitMessage(true).build());
        CommonProcessor.MessagePart messagePart = commonProcessor.getPartsOfMessage(event);
        assertNotNull(messagePart);

        assertEquals(expectedParts, messagePart.parts());
        assertEquals(lengthOfPart, messagePart.messageLength());
    }

    static Stream<Arguments> messageEventsForCalculateMessageParts() {
        String gsm7OneMessageSm = "Integer euismod purus ut magna gravida, ac condimentum dui consequat. Sed felis massa, fermentum nec nisl sodales, tincidunt volutpat dui. Donec iaculis libero.";

        // MessageEvent for GSM 7-bit encoding with one part
        MessageEvent eventGsm7OneMessage = MessageEvent.builder()
                .id("1722442489766-7788604799226")
                .messageId("1722442489770-7788608933795")
                .systemId("smpp_sp")
                .commandStatus(0)
                .segmentSequence(2)
                .sourceAddr("50588888888")
                .destinationAddr("50599999999")
                .validityPeriod(60)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                .dataCoding(0)
                .shortMessage(gsm7OneMessageSm)
                .messageBytes(EncodingUtils.encodeMessage(gsm7OneMessageSm, 0))
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .originNetworkId(1)
                .destNetworkType("GW")
                .destProtocol(GeneralSmscConstants.SS7_PROTOCOL)
                .destNetworkId(4)
                .destAddrNpi(1)
                .destAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddrTon(1)
                .esmClass(3)
                .build();

        // Create a deep copy of the event for two parts
        MessageEvent eventGsm7TwoParts = Converter.deepCopy(eventGsm7OneMessage, MessageEvent.class);
        eventGsm7TwoParts.setShortMessage(gsm7OneMessageSm + "!");
        eventGsm7TwoParts.setMessageBytes(EncodingUtils.encodeMessage(gsm7OneMessageSm, 0));

        // MessageEvent for GSM 7-bit encoding with two parts
        MessageEvent eventGsm7TwoPartsWithUdh = Converter.deepCopy(eventGsm7OneMessage, MessageEvent.class);
        String completeUdhHex = "0605043E940000";
        String udhWithoutUdhl = "05043E940000";
        eventGsm7TwoPartsWithUdh.setUdhBytes(completeUdhHex.getBytes());
        eventGsm7TwoPartsWithUdh.setUdhRaw(Set.of(new Udh("06", udhWithoutUdhl)));
        eventGsm7TwoPartsWithUdh.setUdhLength(6);

        // MessageEvent for UCS2 encoding with one part
        String ucs2OneMessageSm = "Nunc id tincidunt nisi. Vestibulum ante ipsum primis in faucibus quis.";
        MessageEvent eventUcs2OneMessage = Converter.deepCopy(eventGsm7OneMessage, MessageEvent.class);
        eventUcs2OneMessage.setShortMessage(ucs2OneMessageSm);
        byte[] ucs2Bytes = EncodingUtils.encodeMessage(ucs2OneMessageSm, 2);
        eventUcs2OneMessage.setMessageBytes(ucs2Bytes);
        eventUcs2OneMessage.setDataCoding(8);

        // MessageEvent for UCS2 encoding with two parts
        MessageEvent eventUcs2TwoParts = Converter.deepCopy(eventUcs2OneMessage, MessageEvent.class);
        eventUcs2TwoParts.setShortMessage(ucs2OneMessageSm + "!");
        byte[] ucs2TwoPartsBytes = EncodingUtils.encodeMessage((ucs2OneMessageSm + "!"), 2);
        eventUcs2TwoParts.setMessageBytes(ucs2TwoPartsBytes);

        // Create a deep copy of the event for two parts with UDH
        MessageEvent eventUcs2TwoPartsWithUdh = Converter.deepCopy(eventUcs2OneMessage, MessageEvent.class);
        String ucs2UdhHex = "0505043E940000";
        String ucs2UdhWithoutUdhl = "05043E940000";
        eventUcs2TwoPartsWithUdh.setUdhBytes(EncodingUtils.encodeMessage(ucs2UdhHex, 2));
        eventUcs2TwoPartsWithUdh.setUdhRaw(Set.of(new Udh("05", ucs2UdhWithoutUdhl)));
        eventUcs2TwoPartsWithUdh.setUdhLength(6);

        // MessageEvent for Binary encoding with one part
        String binaryOneMessageSm = "Donec viverra mi metus, et commodo libero cursus et. Nullam condimentum sit amet lorem a venenatis. Vestibulum rhoncus commodo sem non quam.";
        byte[] binaryBytes = binaryOneMessageSm.getBytes(StandardCharsets.ISO_8859_1);
        binaryOneMessageSm = EncodingUtils.bytesToHex(binaryBytes);
        MessageEvent eventBinaryOneMessage = Converter.deepCopy(eventGsm7OneMessage, MessageEvent.class);
        eventBinaryOneMessage.setShortMessage(binaryOneMessageSm);
        eventBinaryOneMessage.setMessageBytes(binaryBytes);
        eventBinaryOneMessage.setDataCoding(4);

        // Create a deep copy of the event for two parts
        MessageEvent eventBinaryTwoParts = Converter.deepCopy(eventBinaryOneMessage, MessageEvent.class);
        eventBinaryTwoParts.setShortMessage(binaryOneMessageSm + "21"); // 21 is -> !
        byte[] binaryTwoPartsBytes = EncodingUtils.encodeMessage((binaryOneMessageSm + "21"), 2);
        eventBinaryTwoParts.setMessageBytes(binaryTwoPartsBytes);
        eventBinaryTwoParts.setUdhLength(0); // Binary messages do not have UDH
        eventBinaryTwoParts.setUdhBytes(new byte[0]);

        // Create a deep copy of the event for two parts with UDH
        MessageEvent eventBinaryTwoPartsWithUdh = Converter.deepCopy(eventBinaryOneMessage, MessageEvent.class);
        String binaryUdhHex = "0505043E940000";
        String binaryUdhWithoutUdhl = "05043E940000";
        eventBinaryTwoPartsWithUdh.setUdhBytes(EncodingUtils.encodeMessage(binaryUdhHex, 2));
        eventBinaryTwoPartsWithUdh.setUdhRaw(Set.of(new Udh("05", binaryUdhWithoutUdhl)));
        eventBinaryTwoPartsWithUdh.setUdhLength(6);

        return Stream.of(
                Arguments.of(eventGsm7OneMessage, 1, 160),
                Arguments.of(eventGsm7TwoParts, 2, 152),
                Arguments.of(eventGsm7TwoPartsWithUdh, 2, 144),
                Arguments.of(eventUcs2OneMessage, 1, 70),
                Arguments.of(eventUcs2TwoParts, 2, 66),
                Arguments.of(eventUcs2TwoPartsWithUdh, 2, 63),
                Arguments.of(eventBinaryOneMessage, 1, 140),
                Arguments.of(eventBinaryTwoParts, 2, 133),
                Arguments.of(eventBinaryTwoPartsWithUdh, 2, 127)
        );
    }

    @ParameterizedTest
    @MethodSource("longMessagesCandidatesParams")
    void processCandidatesForLongMessagesTest(
            int dataCoding, String text, boolean splitMessage, String splitBy, int expectedTotalParts, List<Integer> expectedMessageBytesSize) {
        GeneralSettings generalSettings = GeneralSettings.builder()
                .validityPeriod(120)
                .maxValidityPeriod(240)
                .id(1)
                .destAddrNpi(1)
                .destAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddrTon(1)
                .encodingGsm7(0)
                .encodingUcs2(2)
                .encodingIso88591(3)
                .build();

        when(settingsLoader.getSmppHttpSettings()).thenReturn(generalSettings);
        when(gateways.get(anyInt())).thenReturn(Gateway.builder()
                .networkId(2)
                .name("GW")
                .splitMessage(splitMessage)
                .splitSmppType(splitBy)
                .encodingGsm7(EncodingUtils.GSM7)
                .encodingUcs2(EncodingUtils.UCS2)
                .encodingIso88591(EncodingUtils.ISO88591)
                .build());

        MessageEvent event = MessageEvent.builder()
                .destNetworkType("GW")
                .destNetworkId(2)
                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .shortMessage(text)
                .dataCoding(dataCoding)
                .build();

        commonProcessor.processCandidatesForLongMessages(event);

        assertNotNull(event);
        int totalPartsSize = Objects.nonNull(event.getMessageParts()) ? expectedTotalParts : 0;
        assertEquals(expectedTotalParts, totalPartsSize);
        if (splitMessage) {
            List<Integer> groupedMessageBytesSize = event.getMessageParts().stream()
                    .map(x -> x.getPartBytes().length)
                    .toList();
            assertTrue(groupedMessageBytesSize.containsAll(expectedMessageBytesSize));
        } else {
            int encodingType = SmppUtils.determineEncodingType(dataCoding, generalSettings);
            byte[] messageByte = EncodingUtils.encodeMessage(event.getShortMessage(), encodingType);
            assertEquals(expectedMessageBytesSize.getFirst(), messageByte.length);
        }
    }

    static Stream<Arguments> longMessagesCandidatesParams() {
        String textDc0 = "Etiam venenatis sapien semper risus dictum, et ornare dui tincidunt. Aliquam urna orci, venenatis nec cursus quis, maximus sit amet nulla. Curabitur ullamcorper, lacus id auctor eleifend, nisl ante tempor nibh, id gravida mi leo eu turpis. Duis volutpat0.";
        String textDc4 = "Phasellus suscipit a ante tristique aliquet. Maecenas fringilla pulvinar lorem sed maximus. In hac habitasse platea dictumst. Quisque sed purus maximus, accumsan felis ac, fermentum purus. Aliquam sit amet libero et arcu finibus faucibus. Nunc ac tortor0.";
        String textDc8 = "Fusce efficitur tincidunt tortor a auctor. Cras a felis vitae elit imperdiet consectetur. Morbi venenatis nulla eget efficitur0.";

        // dataCoding, shortMessage, splitSms, splitBy, totalParts
        return Stream.of(
                Arguments.of(0, textDc0, true, "UDH", 2, List.of(152, 103)),
                Arguments.of(0, textDc0, true, "TLV", 2, List.of(152, 103)),
                Arguments.of(0, textDc0, false, "UDH", 0, List.of(255)),
                Arguments.of(4, textDc4, true, "UDH", 2, List.of(133, 122)),
                Arguments.of(4, textDc4, true, "TLV", 2, List.of(133, 122)),
                Arguments.of(4, textDc8, false, "UDH", 0, List.of(128)),
                Arguments.of(8, textDc8, true, "UDH", 2, List.of(132, 124)),
                Arguments.of(8, textDc8, true, "TLV", 2, List.of(132, 124)),
                Arguments.of(8, textDc8, false, "UDH", 0, List.of(256))
        );
    }

    static Stream<Arguments> dndScenarios() {
        return Stream.of(
                Arguments.of("GLOBAL", "GLOBAL", true, false, false, ErrorCodes.BLOCKED_BY_DND_GLOBAL),
                Arguments.of("NETWORK_ID", "1", false, true, false, ErrorCodes.BLOCKED_BY_DND_NETWORK),
                Arguments.of("SENDER", "12345678", false, false, true, ErrorCodes.BLOCKED_BY_DND_SENDER)
        );
    }

    @ParameterizedTest
    @MethodSource("dndScenarios")
    @DisplayName("Should block message when destination matches DND list by type")
    void processMessageWhenInDndListThenSkipProcessing(
            String dndType, String dndKey,
            boolean globalStub, boolean networkStub, boolean senderStub,
            int expectedErrorCode) {

        String shortMessage = "hello world";
        MessageEvent messageEvent = MessageEvent.builder()
                .id("1722442489766-7788604799226")
                .messageId("1722442489770-7788608933795")
                .systemId("smpp_sp")
                .commandStatus(0)
                .segmentSequence(2)
                .sourceAddr("12345678")
                .destinationAddr("50512345678")
                .validityPeriod(60)
                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                .dataCoding(0)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .shortMessage(shortMessage)
                .messageBytes(EncodingUtils.encodeMessage(shortMessage, 0))
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .originNetworkId(1)
                .destNetworkType("GW")
                .destProtocol(GeneralSmscConstants.SS7_PROTOCOL)
                .destNetworkId(4)
                .destAddrNpi(1)
                .destAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddrTon(1)
                .esmClass(3)
                .build();

        when(settingsLoader.isCommonSettingEnabled(USE_DND_FILTERING)).thenReturn(true);

        if (globalStub) {
            when(scyllaManager.isInDndWithExactKey("GLOBAL", "50512345678", "GLOBAL")).thenReturn(true);
        } else {
            when(scyllaManager.isInDndWithExactKey("GLOBAL", "50512345678", "GLOBAL")).thenReturn(false);
        }

        if (!globalStub && networkStub) {
            when(scyllaManager.isInDndWithExactKey("NETWORK_ID", "50512345678", "1")).thenReturn(true);
        } else if (!globalStub) {
            when(scyllaManager.isInDndWithExactKey("NETWORK_ID", "50512345678", "1")).thenReturn(false);
        }

        if (!globalStub && !networkStub && senderStub) {
            when(scyllaManager.isInDndWithExactKey("SENDER", "50512345678", "12345678")).thenReturn(true);
        } else if (!globalStub && !networkStub) {
            when(scyllaManager.isInDndWithExactKey("SENDER", "50512345678", "12345678")).thenReturn(false);
        }

        commonProcessor.processMessage(messageEvent);

        verify(scyllaManager).isInDndWithExactKey(dndType, "50512345678", dndKey);
        assertEquals(expectedErrorCode, messageEvent.getErrorCode());
    }

    @Test
    @DisplayName("broadcastWithUdhRejected: sets NOT_SUPPORTED and continues (current behavior)")
    void broadcastWithUdhRejected() {
        primeRuleAndGateway();
        when(settingsLoader.getSmppHttpSettings()).thenReturn(buildGeneral());

        MessageEvent ev = MessageEvent.builder()
                .id("1747167540021-14605739631291")
                .broadcastId(1)
                .originNetworkId(1)
                .originNetworkType("SP")
                .originProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                .destNetworkType("GW")
                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .messageId("mid-udhtest")
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .systemId("broadcast")
                .destinationAddr("12345")
                .shortMessage("Hello")
                .dataCoding(0)
                .esmClass(64)
                .build();

        commonProcessor.processMessage(ev);

        assertEquals(ErrorCodes.NOT_SUPPORTED, ev.getErrorCode().intValue());
        assertEquals(2, ev.getDestNetworkId());

        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
    }

    private GeneralSettings buildGeneral() {
        return GeneralSettings.builder()
                .encodingGsm7(EncodingUtils.GSM7)
                .encodingUcs2(EncodingUtils.UCS2)
                .encodingIso88591(EncodingUtils.ISO88591)
                .validityPeriod(120)
                .maxValidityPeriod(240)
                .build();
    }

    @Test
    @DisplayName("processDlr applies default data coding to SMSC-generated DLR in else-branch")
    void processDlrAppliesDefaultDataCodingForSmscGeneratedDlr() {
        String dlrText = "id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:";
        MessageEvent dlrEvent = MessageEvent.builder()
                .id(System.currentTimeMillis() + "-" + System.nanoTime())
                .messageId(System.currentTimeMillis() + "-" + System.nanoTime())
                .originProtocol("SS7")
                .originNetworkId(5)
                .destProtocol("HTTP")
                .destNetworkId(1)
                .destNetworkType("SP")
                .shortMessage(dlrText)
                .delReceipt(dlrText)
                .dataCoding(8)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();
        dlrEvent.addCustomParam(com.paicbd.smsc.utils.GeneralSmscConstants.SMSC_GENERATED_DLR, true);

        when(appProperties.getSmscDefaultDlrDataCoding()).thenReturn(0);

        commonProcessor.processDlr(dlrEvent);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topicCaptor.capture(), messageCaptor.capture());

        MessageEvent published = Converter.stringToObject(messageCaptor.getValue(), MessageEvent.class);
        assertEquals(0, published.getDataCoding());
        assertNotNull(published.getMessageBytes());
        assertNotNull(published.getSmscMessagePriority());
        assertEquals(dlrEvent.getSmscMessagePriority(), published.getSmscMessagePriority());

        byte[] expectedBytes = EncodingUtils.encodeMessage(dlrText, EncodingUtils.GSM7);
        assertEquals(expectedBytes.length, published.getMessageBytes().length);
    }

    @Test
    @DisplayName("processDlr preserves data coding for non-SMSC-generated DLR in else-branch")
    void processDlrPreservesDataCodingForNonSmscGeneratedDlr() {
        String dlrText = "id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:";
        MessageEvent dlrEvent = MessageEvent.builder()
                .id(System.currentTimeMillis() + "-" + System.nanoTime())
                .messageId(System.currentTimeMillis() + "-" + System.nanoTime())
                .originProtocol("SS7")
                .originNetworkId(5)
                .destProtocol("HTTP")
                .destNetworkId(1)
                .destNetworkType("SP")
                .shortMessage(dlrText)
                .delReceipt(dlrText)
                .dataCoding(8)
                .smscMessagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .build();

        commonProcessor.processDlr(dlrEvent);

        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(anyString(), messageCaptor.capture());

        MessageEvent published = Converter.stringToObject(messageCaptor.getValue(), MessageEvent.class);
        assertEquals(8, published.getDataCoding());
        assertNotNull(published.getSmscMessagePriority());
        assertEquals(dlrEvent.getSmscMessagePriority(), published.getSmscMessagePriority());
    }

    private void primeRuleAndGateway() {
        RoutingRule routingRule = getRuleWithActionsAndSmppDestination();
        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(routingRule.getDestination().getFirst());
        when(gateways.get(anyInt())).thenReturn(
                Gateway.builder()
                        .networkId(2)
                        .name("GW")
                        .requestDLR(RequestDelivery.REQUEST_DLR.getValue())
                        .splitMessage(false)
                        .splitSmppType("UDH")
                        .build()
        );
    }

    @Test
    @DisplayName("processMessage injects SP custom parameters into the MessageEvent before routing")
    void processMessageInjectsSpCustomParametersIntoMessageEvent() {
        com.paicbd.smsc.dto.ServiceProvider sp = new com.paicbd.smsc.dto.ServiceProvider();
        sp.setCustomParameters(Map.of("system_id", "NMB", "pwd", "test123"));
        when(serviceProviders.get(1)).thenReturn(sp);
        RoutingRule routingRule = getRuleWithActionsAndSmppDestination();
        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(routingRule.getDestination().getFirst());
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());
        when(gateways.get(anyInt())).thenReturn(Gateway.builder()
                .name("GW")
                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                .splitSmppType("TLV")
                .build());

        MessageEvent messageEvent = MessageEvent.builder()
                .id(System.currentTimeMillis() + "-" + System.nanoTime())
                .messageId(System.currentTimeMillis() + "-" + System.nanoTime())
                .systemId("nmb_sp")
                .originNetworkId(1)
                .originNetworkType("SP")
                .originProtocol("HTTP")
                .destinationAddr("50599999999")
                .sourceAddr("50588888888")
                .shortMessage("Hello from NMB SP")
                .dataCoding(0)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();

        commonProcessor.processMessage(messageEvent);

        assertEquals("NMB", messageEvent.getFromCustomParam("system_id", null));
        assertEquals("test123", messageEvent.getFromCustomParam("pwd", null));
    }

    @Test
    @DisplayName("processMessage does not inject custom parameters when SP has null customParameters")
    void processMessageDoesNotInjectCustomParametersWhenSpHasNullCustomParameters() {
        com.paicbd.smsc.dto.ServiceProvider sp = new com.paicbd.smsc.dto.ServiceProvider();
        when(serviceProviders.get(1)).thenReturn(sp);
        RoutingRule routingRule = getRuleWithActionsAndSmppDestination();
        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(routingRule.getDestination().getFirst());
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());
        when(gateways.get(anyInt())).thenReturn(Gateway.builder()
                .name("GW")
                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                .splitSmppType("TLV")
                .build());

        MessageEvent messageEvent = MessageEvent.builder()
                .id(System.currentTimeMillis() + "-" + System.nanoTime())
                .messageId(System.currentTimeMillis() + "-" + System.nanoTime())
                .systemId("nmb_sp")
                .originNetworkId(1)
                .originNetworkType("SP")
                .originProtocol("HTTP")
                .destinationAddr("50599999999")
                .sourceAddr("50588888888")
                .shortMessage("Hello from NMB SP")
                .dataCoding(0)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();

        commonProcessor.processMessage(messageEvent);

        assertNull(messageEvent.getFromCustomParam("system_id", null));
        assertNull(messageEvent.getFromCustomParam("pwd", null));
    }

    @Test
    @DisplayName("processMessage does not inject custom parameters when SP is not found in the cache")
    void processMessageDoesNotInjectCustomParametersWhenSpNotFoundInCache() {
        when(serviceProviders.get(1)).thenReturn(null);
        RoutingRule routingRule = getRuleWithActionsAndSmppDestination();
        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(routingRule.getDestination().getFirst());
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());
        when(gateways.get(anyInt())).thenReturn(Gateway.builder()
                .name("GW")
                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                .splitSmppType("TLV")
                .build());

        MessageEvent messageEvent = MessageEvent.builder()
                .id(System.currentTimeMillis() + "-" + System.nanoTime())
                .messageId(System.currentTimeMillis() + "-" + System.nanoTime())
                .systemId("nmb_sp")
                .originNetworkId(1)
                .originNetworkType("SP")
                .originProtocol("HTTP")
                .destinationAddr("50599999999")
                .sourceAddr("50588888888")
                .shortMessage("Hello from NMB SP")
                .dataCoding(0)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();

        commonProcessor.processMessage(messageEvent);

        assertNull(messageEvent.getFromCustomParam("system_id", null));
        assertNull(messageEvent.getFromCustomParam("pwd", null));
    }

    @Test
    @DisplayName("processMessage does not inject SP custom parameters for messages originating from a gateway")
    void processMessageDoesNotInjectSpCustomParametersForGatewayOriginatedMessages() {
        RoutingRule routingRule = getRuleWithActionsAndSmppDestination();
        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(routingRule.getDestination().getFirst());
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());
        when(gateways.get(anyInt())).thenReturn(Gateway.builder()
                .name("GW")
                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                .splitSmppType("TLV")
                .build());

        MessageEvent messageEvent = MessageEvent.builder()
                .id(System.currentTimeMillis() + "-" + System.nanoTime())
                .messageId(System.currentTimeMillis() + "-" + System.nanoTime())
                .systemId("smpp_gw")
                .originNetworkId(2)
                .originNetworkType("GW")
                .originProtocol("SMPP")
                .destinationAddr("50588888888")
                .sourceAddr("50599999999")
                .shortMessage("id:msg-001 sub:001 dlvrd:001 submit date:2401010000 done date:2401010001 stat:DELIVRD err:000")
                .dataCoding(0)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();

        commonProcessor.processMessage(messageEvent);

        assertNull(messageEvent.getFromCustomParam("system_id", null));
        assertNull(messageEvent.getFromCustomParam("pwd", null));
    }

    @Test
    @DisplayName("processMessage should send failed CDR and stop when message is blocked by DND")
    void processMessageWhenBlockedByDndThenSendFailedCdr() {
        MessageEvent messageEvent = getSingleMessage();
        messageEvent.setDestinationAddr("50512345678");

        when(settingsLoader.isCommonSettingEnabled(USE_DND_FILTERING)).thenReturn(true);
        when(scyllaManager.isInDndWithExactKey("GLOBAL", "50512345678", "GLOBAL")).thenReturn(true);

        commonProcessor.processMessage(messageEvent);

        assertEquals(ErrorCodes.BLOCKED_BY_DND_GLOBAL, messageEvent.getErrorCode());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CDR_TOPIC), anyString());
        verify(routingMatcher, never()).getRouting(any());
    }

    @Test
    @DisplayName("processMessage should send failed CDR when no routing is found")
    void processMessageWhenNoRoutingThenSendFailedCdr() {
        MessageEvent messageEvent = getSingleMessage();
        when(routingMatcher.getRouting(any())).thenReturn(null);

        commonProcessor.processMessage(messageEvent);

        assertEquals(ErrorCodes.NOT_ROUTING, messageEvent.getErrorCode());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CDR_TOPIC), anyString());
    }

    @Test
    @DisplayName("processMessage should send failed CDR when no destination is found")
    void processMessageWhenNoDestinationThenSendFailedCdr() {
        MessageEvent messageEvent = getSingleMessage();
        RoutingRule routingRule = getRuleWithActionsAndSmppDestination();
        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(null);

        commonProcessor.processMessage(messageEvent);

        assertEquals(ErrorCodes.NOT_DESTINATION, messageEvent.getErrorCode());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CDR_TOPIC), anyString());
    }

    @Test
    @DisplayName("processMessage should route and publish a SMPP message when not a retry")
    void processMessageWhenSmppDestinationThenPublish() {
        MessageEvent messageEvent = getSingleMessage();
        // Force the default ESM class resolution branch (null esmClass with UDH present)
        messageEvent.setEsmClass(null);
        messageEvent.setUdhLength(5);
        RoutingRule routingRule = getRuleWithActionsAndSmppDestination();
        RoutingRule.Destination destination = routingRule.getDestination().getFirst();

        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(destination);
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());
        when(gateways.get(anyInt())).thenReturn(Gateway.builder()
                .name("GW")
                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                .splitSmppType("TLV")
                .build());

        commonProcessor.processMessage(messageEvent);

        assertEquals(routingRule.getId(), messageEvent.getRoutingId());
        assertEquals(destination.getNetworkId(), messageEvent.getDestNetworkId());
        assertEquals(GeneralSmscConstants.SMPP_PROTOCOL, messageEvent.getDestProtocol());
        assertEquals(64, messageEvent.getEsmClass());
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("processMessage should use alternative routing when the message is a retry")
    void processMessageWhenRetryThenUseAlternativeRouting() {
        MessageEvent messageEvent = getSingleMessage();
        messageEvent.setRetry(true);
        RoutingRule routingRule = getRuleWithActionsAndSmppDestination();
        RoutingRule.Destination destination = routingRule.getDestination().getFirst();

        when(routingMatcher.getRoutingForAlternative(messageEvent)).thenReturn(routingRule);
        when(routingMatcher.getDestinationForAlternative(routingRule, messageEvent)).thenReturn(destination);
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());
        when(gateways.get(anyInt())).thenReturn(Gateway.builder()
                .name("GW")
                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                .splitSmppType("TLV")
                .build());

        commonProcessor.processMessage(messageEvent);

        verify(routingMatcher).getRoutingForAlternative(messageEvent);
        verify(routingMatcher).getDestinationForAlternative(routingRule, messageEvent);
        verify(routingMatcher, never()).getRouting(any());
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("processMessage should fail when SIP destination has no registration and no fallback")
    void processMessageWhenSipDestinationNotRegisteredAndNoFallbackThenSendFailedCdr() {
        MessageEvent messageEvent = getSingleMessage();
        RoutingRule routingRule = getRuleWithActionsAndSipDestination();
        RoutingRule.Destination destination = routingRule.getDestination().getFirst();

        // An inactive registration is discarded (treated as not registered)
        MessageRegister inactiveRegistration = MessageRegister.builder()
                .msisdn("50599999999")
                .aor("sip:user@domain")
                .regEventState(RegEventState.INACTIVE)
                .build();

        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(destination);
        when(scyllaManager.getSipRegistrationByMsisdn(anyString())).thenReturn(inactiveRegistration);
        when(routingMatcher.validateAndGetNextNonSipDestination(any(RoutingRule.Destination.class), anyList(), any(MessageEvent.class), nullable(MessageRegister.class)))
                .thenReturn(null);

        commonProcessor.processMessage(messageEvent);

        assertEquals(ErrorCodes.NOT_DESTINATION_ADDR_REGISTERED, messageEvent.getErrorCode());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CDR_TOPIC), anyString());
    }

    @Test
    @DisplayName("processMessage should keep SIP destination when registration is active")
    void processMessageWhenSipDestinationRegisteredThenKeepSipDestination() {
        MessageEvent messageEvent = getSingleMessage();
        RoutingRule routingRule = getRuleWithActionsAndSipDestination();
        RoutingRule.Destination sipDestination = routingRule.getDestination().getFirst();

        MessageRegister registerData = MessageRegister.builder()
                .msisdn("50599999999")
                .aor("sip:user@domain")
                .regEventState(RegEventState.ACTIVE)
                .build();

        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(sipDestination);
        when(scyllaManager.getSipRegistrationByMsisdn(anyString())).thenReturn(registerData);
        when(routingMatcher.validateAndGetNextNonSipDestination(any(RoutingRule.Destination.class), anyList(), any(MessageEvent.class), nullable(MessageRegister.class)))
                .thenReturn(sipDestination);
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());
        when(sipGatewaysConcurrentMap.get(anyInt())).thenReturn(SipGateways.builder()
                .networkId(sipDestination.getNetworkId())
                .name("sipGateways")
                .build());

        commonProcessor.processMessage(messageEvent);

        assertEquals(GeneralSmscConstants.SIP_PROTOCOL, messageEvent.getDestProtocol());
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("processMessage should publish to the charging topic when diameter charging applies and it is not a retry")
    void processMessageWhenDiameterChargingAndNotRetryThenPublishToChargingTopic() {
        MessageEvent messageEvent = getSingleMessage();
        RoutingRule routingRule = getRuleWithActionsAndDiameterDestination();
        routingRule.setDiameterCharging(true);
        RoutingRule.Destination destination = routingRule.getDestination().getFirst();

        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(destination);
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());
        when(diameterGatewayMap.get(anyInt())).thenReturn(DiameterConfig.builder()
                .networkId(destination.getNetworkId())
                .name("diameter")
                .globalTitle("50588776655")
                .build());

        commonProcessor.processMessage(messageEvent);

        assertTrue(messageEvent.isDiameterCharging());
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topicCaptor.capture(), anyString());
        assertEquals(KafkaUtils.getChargingTopicPriority(messageEvent.getSmscMessagePriority()), topicCaptor.getValue());
    }

    @Test
    @DisplayName("processMessage should not publish to charging topic for a retry even when diameter charging applies")
    void processMessageWhenDiameterChargingAndRetryThenPublishToDestinationTopic() {
        MessageEvent messageEvent = getSingleMessage();
        messageEvent.setRetry(true);
        RoutingRule routingRule = getRuleWithActionsAndDiameterDestination();
        routingRule.setDiameterCharging(true);
        RoutingRule.Destination destination = routingRule.getDestination().getFirst();

        when(routingMatcher.getRoutingForAlternative(messageEvent)).thenReturn(routingRule);
        when(routingMatcher.getDestinationForAlternative(routingRule, messageEvent)).thenReturn(destination);
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());
        when(diameterGatewayMap.get(anyInt())).thenReturn(DiameterConfig.builder()
                .networkId(destination.getNetworkId())
                .name("diameter")
                .globalTitle("50588776655")
                .build());

        commonProcessor.processMessage(messageEvent);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topicCaptor.capture(), anyString());
        assertNotEquals(KafkaUtils.getChargingTopicPriority(messageEvent.getSmscMessagePriority()), topicCaptor.getValue());
    }

    @Test
    @DisplayName("processMessage should set SS7 properties from settings for a SS7 destination")
    void processMessageWhenSs7DestinationThenSetSs7Properties() {
        MessageEvent messageEvent = getSingleMessage();
        RoutingRule routingRule = getRuleWithActionsAndSs7Destination();
        RoutingRule.Destination destination = routingRule.getDestination().getFirst();

        Ss7Settings ss7Settings = Ss7Settings.builder()
                .name("ss7")
                .networkId(destination.getNetworkId())
                .globalTitle("50588655545")
                .globalTitleIndicator(UtilsEnum.GlobalTitleIndicator.GT0100)
                .translationType(0)
                .smscSsn(8)
                .hlrSsn(6)
                .mscSsn(8)
                .mapVersion(3)
                .splitMessage(true)
                .build();

        when(routingMatcher.getRouting(any())).thenReturn(routingRule);
        when(routingMatcher.getDestinationByRouting(routingRule)).thenReturn(destination);
        when(settingsLoader.getSs7Settings(anyInt())).thenReturn(ss7Settings);
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());
        when(appProperties.getTlvSccpCalledAddrSri()).thenReturn((short) -1);

        commonProcessor.processMessage(messageEvent);

        assertEquals(GeneralSmscConstants.SS7_PROTOCOL, messageEvent.getDestProtocol());
        // The SS7 routing action with template "{{destinationAddr}}" replaces the global title
        assertEquals(messageEvent.getDestinationAddr(), messageEvent.getGlobalTitle());
        assertEquals(8, messageEvent.getSmscSsn());
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("processDlrInAsync should publish the DLR through the else-branch when no submit-sm correlation is required")
    void processDlrInAsyncWhenNoSubmitSmCheckThenPublishDlr() {
        MessageEvent dlrEvent = MessageEvent.builder()
                .id(System.currentTimeMillis() + "-" + System.nanoTime())
                .messageId(System.currentTimeMillis() + "-" + System.nanoTime())
                .originProtocol(GeneralSmscConstants.SS7_PROTOCOL)
                .originNetworkType("GW")
                .originNetworkId(2)
                .destProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                .destNetworkType("SP")
                .destNetworkId(1)
                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:")
                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:")
                .status("DELIVRD")
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();

        commonProcessor.processDlrInAsync(dlrEvent);

        assertTrue(dlrEvent.isDlr());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.HTTP_DLR_TOPIC), anyString());
    }

    @Test
    @DisplayName("processDlr should publish DLR for a SMPP originated message with a stored submit-sm response")
    void processDlrWhenSmppOriginThenPublishDlr() {
        MessageEvent dlrEvent = buildDlrToProcessAction(GeneralSmscConstants.SMPP_PROTOCOL, "DELIVRD");
        UtilsRecords.SubmitSmResponseEvent result = buildSubmitSmResponse(
                GeneralSmscConstants.SMPP_PROTOCOL, "SP", false);

        when(scyllaManager.selectFromTable(anyString(), anyString())).thenReturn(result.toString());
        when(settingsLoader.getSmppHttpSettings()).thenReturn(new GeneralSettings());

        commonProcessor.processDlr(dlrEvent);

        assertTrue(dlrEvent.isDlr());
        verify(scyllaManager).deleteFromTable(anyString(), anyString());
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("processDlr should publish DLR for a HTTP originated message with a stored submit-sm response")
    void processDlrWhenHttpOriginThenPublishDlr() {
        MessageEvent dlrEvent = buildDlrToProcessAction(GeneralSmscConstants.HTTP_PROTOCOL, "DELIVRD");
        UtilsRecords.SubmitSmResponseEvent result = buildSubmitSmResponse(
                GeneralSmscConstants.HTTP_PROTOCOL, "SP", false);

        when(scyllaManager.selectFromTable(anyString(), anyString())).thenReturn(result.toString());

        commonProcessor.processDlr(dlrEvent);

        assertEquals("http_sp", dlrEvent.getSystemId());
        assertNotNull(dlrEvent.getDeliverSmServerId());
        assertEquals(RequestDelivery.NON_REQUEST_DLR.getValue(), dlrEvent.getRegisteredDelivery());
        verify(scyllaManager).deleteFromTable(anyString(), anyString());
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
        // Non-proxy: the receipt id is the submit-sm server id from the stored response
        assertTrue(dlrEvent.getDelReceipt().startsWith("id:" + result.submitSmServerId() + " "));
    }

    @Test
    @DisplayName("processDlr should use the deliverSmId as the receipt id for a HTTP DLR when the message uses proxy")
    void processDlrWhenHttpOriginAndUseProxyThenReceiptIdIsDeliverSmId() {
        MessageEvent dlrEvent = buildDlrToProcessAction(GeneralSmscConstants.HTTP_PROTOCOL, "DELIVRD");
        dlrEvent.setUseProxy(true);
        String proxyDeliverSmId = dlrEvent.getDeliverSmId();
        UtilsRecords.SubmitSmResponseEvent result = buildSubmitSmResponse(
                GeneralSmscConstants.HTTP_PROTOCOL, "SP", false);

        when(scyllaManager.selectFromTable(anyString(), anyString())).thenReturn(result.toString());

        commonProcessor.processDlr(dlrEvent);

        verify(scyllaManager).deleteFromTable(anyString(), anyString());
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
        // Proxy: the receipt id must be the deliverSmId, not the submit-sm server id
        assertTrue(dlrEvent.getDelReceipt().startsWith("id:" + proxyDeliverSmId + " "));
        assertTrue(dlrEvent.getShortMessage().startsWith("id:" + proxyDeliverSmId + " "));
        assertFalse(dlrEvent.getDelReceipt().contains("id:" + result.submitSmServerId() + " "));
    }

    @Test
    @DisplayName("processDlr should return without publishing when the submit-sm response is not found")
    void processDlrWhenResultIsNullThenReturnWithoutPublishing() {
        MessageEvent dlrEvent = buildDlrToProcessAction(GeneralSmscConstants.SMPP_PROTOCOL, "DELIVRD");
        when(scyllaManager.selectFromTable(anyString(), anyString())).thenReturn(null);

        commonProcessor.processDlr(dlrEvent);

        verify(scyllaManager, never()).deleteFromTable(anyString(), anyString());
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    @DisplayName("processDlr should ignore DLR when origin is a SMPP gateway and write a failed CDR")
    void processDlrWhenSmppGatewayOriginThenIgnoreAndSendFailedCdr() {
        MessageEvent dlrEvent = buildDlrToProcessAction(GeneralSmscConstants.SMPP_PROTOCOL, "DELIVRD");
        UtilsRecords.SubmitSmResponseEvent result = buildSubmitSmResponse(
                GeneralSmscConstants.SMPP_PROTOCOL, "GW", false);

        when(scyllaManager.selectFromTable(anyString(), anyString())).thenReturn(result.toString());

        commonProcessor.processDlr(dlrEvent);

        assertFalse(dlrEvent.isProcess());
        assertEquals(ErrorCodes.NOT_SUPPORTED, dlrEvent.getErrorCode());
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CDR_TOPIC), anyString());
    }

    @Test
    @DisplayName("processDlr should publish a refund message when the DLR final state is a failed-for-refund status")
    void processDlrWhenFailedStatusAndApplyForRefundThenPublishRefund() {
        MessageEvent dlrEvent = buildDlrToProcessAction(GeneralSmscConstants.HTTP_PROTOCOL, "UNDELIV");
        UtilsRecords.SubmitSmResponseEvent result = buildSubmitSmResponse(
                GeneralSmscConstants.HTTP_PROTOCOL, "SP", true);

        when(scyllaManager.selectFromTable(anyString(), anyString())).thenReturn(result.toString());

        commonProcessor.processDlr(dlrEvent);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, atLeastOnce()).send(topicCaptor.capture(), anyString());
        assertTrue(topicCaptor.getAllValues().contains(
                KafkaUtils.getChargingTopicPriority(dlrEvent.getSmscMessagePriority())));
    }

    @Test
    @DisplayName("processDlr should process a SIP DLR and publish it when the correlation exists")
    void processDlrWhenSipOriginAndCorrelationExistsThenPublishDlr() {
        MessageEvent dlrEvent = buildDlrToProcessAction(GeneralSmscConstants.SIP_PROTOCOL, "DELIVRD");
        dlrEvent.setErrorCode(0);

        MessageEvent originalMessage = MessageEvent.builder()
                .id(System.currentTimeMillis() + "-" + System.nanoTime())
                .messageId(System.currentTimeMillis() + "-" + System.nanoTime())
                .originProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                .originNetworkType("SP")
                .originNetworkId(1)
                .destProtocol(GeneralSmscConstants.SIP_PROTOCOL)
                .destNetworkType("GW")
                .destNetworkId(2)
                .sourceAddr("50588888888")
                .destinationAddr("50599999999")
                .shortMessage("Hello SIP")
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .applyForRefund(false)
                .build();

        when(scyllaManager.selectFromTable(anyString(), anyString())).thenReturn(originalMessage.toString());

        commonProcessor.processDlr(dlrEvent);

        verify(scyllaManager).deleteFromTable(anyString(), anyString());
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
    }
    @Test
    @DisplayName("processDlr should skip SIP DLR when correlated message is not the final split segment")
    void processDlrWhenSipOriginAndNonFinalSplitSegmentThenSkipDlr() {
        MessageEvent dlrEvent = buildDlrToProcessAction(GeneralSmscConstants.SIP_PROTOCOL, "DELIVRD");
        dlrEvent.setDeliverSmId("sip-call-id-segment-1");
        dlrEvent.setErrorCode(0);

        MessageEvent originalMessage = MessageEvent.builder()
                .id(System.currentTimeMillis() + "-" + System.nanoTime())
                .messageId("submit-part-1")
                .parentId("parent-message-id")
                .originProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                .originNetworkType("SP")
                .originNetworkId(1)
                .destProtocol(GeneralSmscConstants.SIP_PROTOCOL)
                .destNetworkType("GW")
                .destNetworkId(2)
                .sourceAddr("50588888888")
                .destinationAddr("50599999999")
                .shortMessage("Hello SIP part 1")
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .applyForRefund(false)
                .splitForSmsc(true)
                .msgReferenceNumber("10")
                .segmentSequence(1)
                .totalSegment(2)
                .build();

        when(scyllaManager.selectFromTable(anyString(), eq("sip-call-id-segment-1")))
                .thenReturn(originalMessage.toString());

        commonProcessor.processDlr(dlrEvent);

        verify(scyllaManager).deleteFromTable(anyString(), eq("sip-call-id-segment-1"));
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("processDlr should publish SIP DLR when correlated message is the final split segment")
    void processDlrWhenSipOriginAndFinalSplitSegmentThenPublishDlr() {
        MessageEvent dlrEvent = buildDlrToProcessAction(GeneralSmscConstants.SIP_PROTOCOL, "DELIVRD");
        dlrEvent.setDeliverSmId("sip-call-id-segment-2");
        dlrEvent.setErrorCode(0);

        MessageEvent originalMessage = MessageEvent.builder()
                .id(System.currentTimeMillis() + "-" + System.nanoTime())
                .messageId("submit-part-2")
                .parentId("parent-message-id")
                .originProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                .originNetworkType("SP")
                .originNetworkId(1)
                .destProtocol(GeneralSmscConstants.SIP_PROTOCOL)
                .destNetworkType("GW")
                .destNetworkId(2)
                .sourceAddr("50588888888")
                .destinationAddr("50599999999")
                .shortMessage("Hello SIP part 2")
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .applyForRefund(false)
                .splitForSmsc(true)
                .msgReferenceNumber("10")
                .segmentSequence(2)
                .totalSegment(2)
                .build();

        when(scyllaManager.selectFromTable(anyString(), eq("sip-call-id-segment-2")))
                .thenReturn(originalMessage.toString());

        commonProcessor.processDlr(dlrEvent);

        verify(scyllaManager).deleteFromTable(anyString(), eq("sip-call-id-segment-2"));
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString());
    }

    @Test
    @DisplayName("processDlr should not publish a SIP DLR when the correlation does not exist")
    void processDlrWhenSipOriginAndCorrelationMissingThenDoNothing() {
        MessageEvent dlrEvent = buildDlrToProcessAction(GeneralSmscConstants.SIP_PROTOCOL, "DELIVRD");
        when(scyllaManager.selectFromTable(anyString(), anyString())).thenReturn(null);

        commonProcessor.processDlr(dlrEvent);

        verify(scyllaManager, never()).deleteFromTable(anyString(), anyString());
        verifyNoInteractions(kafkaTemplate);
    }

    private static MessageEvent buildDlrToProcessAction(String originProtocol, String status) {
        String id = System.currentTimeMillis() + "-" + System.nanoTime();
        return MessageEvent.builder()
                .id(id)
                .messageId(id)
                .deliverSmId(id)
                .systemId("http_sp")
                .originProtocol(originProtocol)
                .originNetworkType("SP")
                .originNetworkId(1)
                .destProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .destNetworkType("GW")
                .destNetworkId(4)
                .sourceAddr("50588888888")
                .destinationAddr("50599999999")
                .dataCoding(0)
                .registeredDelivery(RequestDelivery.REQUEST_DLR.getValue())
                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:" + status + " err:000 text:Test Message")
                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:" + status + " err:000 text:Test Message")
                .status(status)
                .checkSubmitSmResponse(true)
                .process(true)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();
    }

    private static UtilsRecords.SubmitSmResponseEvent buildSubmitSmResponse(
            String originProtocol, String originNetworkType, boolean applyForRefund) {
        String id = System.currentTimeMillis() + "-" + System.nanoTime();
        HashMap<String, Object> customParams = new HashMap<>();
        customParams.put(GeneralSmscConstants.MESSAGE_PRIORITY, GeneralSmscConstants.MEDIUM_PRIORITY);
        return new UtilsRecords.SubmitSmResponseEvent(
                "",
                id,
                "http_sp",
                id,
                id,
                originProtocol,
                1,
                originNetworkType,
                "",
                0,
                0,
                id,
                4,
                applyForRefund,
                customParams,
                false,
                "",
                "",
                GeneralSmscConstants.MEDIUM_PRIORITY
        );
    }
}