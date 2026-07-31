package paicbd.smsc.routing.component;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessageRegister;
import com.paicbd.smsc.dto.RoutingRule;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class RoutingMatcherTest {
    @Mock
    private ConcurrentMap<Integer, List<RoutingRule>> routingRules;

    @InjectMocks
    private RoutingMatcher routingMatcher;

    @Test
    @DisplayName("getRouting without rules for network id")
    void getRoutingWhenDoesNotExistRulesForNetworkId() {
        MessageEvent messageEvent = MessageEvent.builder()
                .originNetworkId(1)
                .build();

        RoutingRule foundRoutingRule = routingMatcher.getRouting(messageEvent);
        verify(routingRules).get(1);
        assertNull(foundRoutingRule);
    }

    @Test
    @DisplayName("getRouting when routing has no filter rules")
    void getRoutingWhenRoutingHasNotFilterRules() {
        RoutingRule.Destination destination = new RoutingRule.Destination();
        destination.setPriority(1);
        destination.setNetworkId(2);
        destination.setProtocol(GeneralSmscConstants.SMPP_PROTOCOL);
        destination.setNetworkType("GW");

        RoutingRule routingRule = RoutingRule.builder()
                .originNetworkId(1)
                .destination(List.of(destination))
                .hasFilterRules(false)
                .build();

        MessageEvent messageEvent = MessageEvent.builder()
                .originNetworkId(1)
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .build();

        routingRules = spy(new ConcurrentHashMap<>());
        routingRules.put(1, List.of(routingRule));
        routingMatcher = new RoutingMatcher(routingRules);

        RoutingRule foundRoutingRule = routingMatcher.getRouting(messageEvent);
        verify(routingRules, times(2)).get(1);

        assertNotNull(foundRoutingRule);
        assertEquals(1, foundRoutingRule.getDestination().size());
    }

    @ParameterizedTest
    @MethodSource("allRoutingVariations")
    @DisplayName("getRouting parameterized cases")
    void getRouting(MessageEvent messageEvent, RoutingRule routingRule, boolean expectedNull) {
        routingRules = spy(new ConcurrentHashMap<>());
        routingRules.put(1, List.of(routingRule));
        routingMatcher = new RoutingMatcher(routingRules);

        RoutingRule foundRoutingRule = routingMatcher.getRouting(messageEvent);
        verify(routingRules, atLeast(1)).get(1);

        if (expectedNull) {
            assertNull(foundRoutingRule);
            return;
        }

        assertNotNull(foundRoutingRule);
        assertEquals(1, foundRoutingRule.getDestination().size());

        RoutingRule.Destination destination = foundRoutingRule.getDestination().getFirst();
        assertEquals(1, destination.getPriority());
        assertEquals(2, destination.getNetworkId());
        assertEquals(GeneralSmscConstants.SMPP_PROTOCOL, destination.getProtocol());
        assertEquals("GW", destination.getNetworkType());
        assertEquals(routingRule.toString(), foundRoutingRule.toString());
    }

    static Stream<Arguments> allRoutingVariations() {
        List<RoutingRule.Destination> destinations =
                List.of(new RoutingRule.Destination(1, 2, GeneralSmscConstants.SMPP_PROTOCOL, "GW"));

        // MessageEvent, RoutingRule, ExpectedNullWhileFoundRoutingRule
        return Stream.of(
                Arguments.of(
                        MessageEvent.builder()
                                .originNetworkId(1)
                                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .sourceAddr("50580808080")
                                .sourceAddrTon(1)
                                .sourceAddrNpi(1)
                                .destinationAddr("50581818181")
                                .destAddrTon(1)
                                .destAddrNpi(1)
                                .imsi("4400127890123")
                                .networkNodeNumber("23")
                                .sccpCallingPartyAddress("0123456789")
                                .sriResponse(true)
                                .build(),
                        RoutingRule.builder()
                                .originRegexSourceAddr("^505\\d*")
                                .originRegexSourceAddrTon("^1\\d*")
                                .originRegexSourceAddrNpi("^1\\d*")
                                .originRegexDestinationAddr("^505\\d*")
                                .originRegexDestAddrTon("^1\\d*")
                                .originRegexDestAddrNpi("^1\\d*")
                                .regexImsiDigitsMask("^44\\d*")
                                .regexNetworkNodeNumber("^23\\d*")
                                .regexCallingPartyAddress("^012\\d*")
                                .sriResponse(true)
                                .destination(destinations)
                                .hasFilterRules(true)
                                .build(),
                        false
                ),
                Arguments.of(
                        MessageEvent.builder()
                                .originNetworkId(1)
                                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .sourceAddr("50580808080")
                                .sourceAddrTon(1)
                                .sourceAddrNpi(1)
                                .destinationAddr("50581818181")
                                .destAddrTon(1)
                                .destAddrNpi(1)
                                .imsi("4400127890123")
                                .networkNodeNumber("23")
                                .sccpCallingPartyAddress("0123456789")
                                .sriResponse(true)
                                .build(),
                        RoutingRule.builder()
                                .originRegexSourceAddr("^505\\d*")
                                .originRegexSourceAddrTon("^1\\d*")
                                .originRegexSourceAddrNpi("^1\\d*")
                                .originRegexDestinationAddr("^506\\d*")
                                .originRegexDestAddrTon("^1\\d*")
                                .originRegexDestAddrNpi("^1\\d*")
                                .regexImsiDigitsMask("^44\\d*")
                                .regexNetworkNodeNumber("^23\\d*")
                                .regexCallingPartyAddress("^012\\d*")
                                .sriResponse(true)
                                .destination(destinations)
                                .hasFilterRules(true)
                                .build(),
                        true
                ),
                Arguments.of(
                        MessageEvent.builder()
                                .originNetworkId(1)
                                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .sourceAddr("50580808080")
                                .sourceAddrTon(1)
                                .sourceAddrNpi(1)
                                .destinationAddr("50581818181")
                                .destAddrTon(1)
                                .destAddrNpi(1)
                                .imsi("4400127890123")
                                .networkNodeNumber("23")
                                .sccpCallingPartyAddress("0123456789")
                                .sriResponse(true)
                                .shortMessage("Your OTP is 123456")
                                .build(),
                        RoutingRule.builder()
                                .originRegexSourceAddr("")
                                .originRegexSourceAddrTon("^1\\d*")
                                .originRegexSourceAddrNpi("^1\\d*")
                                .originRegexDestinationAddr("^506\\d*")
                                .originRegexDestAddrTon("^1\\d*")
                                .originRegexDestAddrNpi("^1\\d*")
                                .regexImsiDigitsMask("^44\\d*")
                                .regexNetworkNodeNumber("^23\\d*")
                                .regexCallingPartyAddress("^012\\d*")
                                .regexShortMessage("(?i).*OTP.*")
                                .sriResponse(true)
                                .destination(destinations)
                                .hasFilterRules(true)
                                .build(),
                        true
                ),
                Arguments.of(
                        MessageEvent.builder()
                                .originNetworkId(1)
                                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .sourceAddr("50580808080")
                                .sourceAddrTon(1)
                                .sourceAddrNpi(1)
                                .destinationAddr("50581818181")
                                .destAddrTon(1)
                                .destAddrNpi(1)
                                .imsi("4400127890123")
                                .networkNodeNumber("23")
                                .sccpCallingPartyAddress("0123456789")
                                .sriResponse(true)
                                .build(),
                        RoutingRule.builder()
                                .originRegexSourceAddr("")
                                .originRegexSourceAddrTon("^1\\d*")
                                .originRegexSourceAddrNpi("^1\\d*")
                                .originRegexDestinationAddr("^506\\d*")
                                .originRegexDestAddrTon("^1\\d*")
                                .originRegexDestAddrNpi("^1\\d*")
                                .regexImsiDigitsMask("^44\\d*")
                                .regexNetworkNodeNumber("^23\\d*")
                                .regexCallingPartyAddress("^012\\d*")
                                .sriResponse(true)
                                .destination(destinations)
                                .hasFilterRules(true)
                                .build(),
                        true
                ),
                Arguments.of(
                        MessageEvent.builder()
                                .originNetworkId(1)
                                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .sourceAddr("50580808080")
                                .sourceAddrTon(1)
                                .sourceAddrNpi(1)
                                .destinationAddr("50581818181")
                                .destAddrTon(1)
                                .destAddrNpi(1)
                                .imsi("4400127890123")
                                .networkNodeNumber("23")
                                .sccpCallingPartyAddress("0123456789")
                                .customParams(getCustomParamsForMessage())
                                .build(),
                        RoutingRule.builder()
                                .originNetworkId(1)
                                .originRegexSourceAddr("")
                                .originRegexSourceAddrTon("")
                                .originRegexSourceAddrNpi("")
                                .originRegexDestinationAddr("")
                                .originRegexDestAddrTon("")
                                .originRegexDestAddrNpi("")
                                .regexImsiDigitsMask("")
                                .regexNetworkNodeNumber("")
                                .regexCallingPartyAddress("")
                                .destination(destinations)
                                .customParamsMatcher(
                                        getCustomParamMatcherList(true, false, "ABDF-107", 505))
                                .hasFilterRules(true)
                                .build(),
                        false
                ),
                Arguments.of(
                        MessageEvent.builder()
                                .originNetworkId(1)
                                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                                .sourceAddr("50580808080")
                                .sourceAddrTon(1)
                                .sourceAddrNpi(1)
                                .destinationAddr("50581818181")
                                .destAddrTon(1)
                                .destAddrNpi(1)
                                .imsi("4400127890123")
                                .networkNodeNumber("23")
                                .sccpCallingPartyAddress("0123456789")
                                .customParams(getCustomParamsForMessage())
                                .build(),
                        RoutingRule.builder()
                                .originNetworkId(1)
                                .originRegexSourceAddr("")
                                .originRegexSourceAddrTon("")
                                .originRegexSourceAddrNpi("")
                                .originRegexDestinationAddr("")
                                .originRegexDestAddrTon("")
                                .originRegexDestAddrNpi("")
                                .regexImsiDigitsMask("")
                                .regexNetworkNodeNumber("")
                                .regexCallingPartyAddress("")
                                .destination(destinations)
                                .customParamsMatcher(
                                        getCustomParamMatcherList(false, true, "ABDE-107", 508))
                                .hasFilterRules(true)
                                .build(),
                        true
                )
        );
    }

    private static Map<String, Object> getCustomParamsForMessage() {
        return Map.of("external",
                Map.ofEntries(
                        Map.entry("isVip", true),
                        Map.entry("isFake", false),
                        Map.entry("priorityRule", "ABDF-107"),
                        Map.entry("deep", Map.ofEntries(Map.entry("prefix", 505)))
                )
        );
    }

    private static List<RoutingRule.CustomParamMatcher> getCustomParamMatcherList(
            Object isVipMatcher, Object isFakeMatcher, Object priorityRuleMatcher, Object deepPrefixMatcher) {
        return List.of(
                new RoutingRule.CustomParamMatcher("customParams.external.isVip", isVipMatcher),
                new RoutingRule.CustomParamMatcher("customParams.external.isFake", isFakeMatcher),
                new RoutingRule.CustomParamMatcher("external.priorityRule", priorityRuleMatcher), // the customParam prefix is ignored if exists
                new RoutingRule.CustomParamMatcher("external.deep.prefix", deepPrefixMatcher)
        );
    }

    @Test
    @DisplayName("validate SIP destination when SIP supported")
    void validateAndGetNextNonSipDestinationWhenSipIsSupportedThenKeepCurrentDestination() {
        RoutingRule.Destination currentDestination = new RoutingRule.Destination(1, 2, GeneralSmscConstants.SIP_PROTOCOL, "GW");
        RoutingRule.Destination secondDestination = new RoutingRule.Destination(2, 3, GeneralSmscConstants.SMPP_PROTOCOL, "GW");
        MessageEvent event = MessageEvent.builder()
                .destinationAddr("50599999999")
                .retryDestNetworkId("")
                .destNetworkId(2)
                .build();
        MessageRegister registerData = MessageRegister.builder()
                .msisdn("50599999999")
                .aor("sip:user@domain")
                .build();

        RoutingRule.Destination result = routingMatcher.validateAndGetNextNonSipDestination(
                currentDestination,
                List.of(currentDestination, secondDestination),
                event,
                registerData
        );

        assertEquals(currentDestination, result);
        assertTrue(event.isSipMessageSupported());
        assertEquals("sip:user@domain", event.getDestinationUri());
    }

    @Test
    @DisplayName("validate non-SIP destination when SIP not supported")
    void validateAndGetNextNonSipDestinationWhenSipIsNotSupportedThenReturnNextNonSipDestination() {
        RoutingRule.Destination currentDestination = new RoutingRule.Destination(1, 2, GeneralSmscConstants.SIP_PROTOCOL, "GW");
        RoutingRule.Destination secondDestination = new RoutingRule.Destination(2, 3, GeneralSmscConstants.SIP_PROTOCOL, "GW");
        RoutingRule.Destination thirdDestination = new RoutingRule.Destination(3, 4, GeneralSmscConstants.SMPP_PROTOCOL, "GW");
        MessageEvent event = MessageEvent.builder()
                .destinationAddr("50599999999")
                .retryDestNetworkId("")
                .build();

        RoutingRule.Destination result = routingMatcher.validateAndGetNextNonSipDestination(
                currentDestination,
                List.of(currentDestination, secondDestination, thirdDestination),
                event,
                null
        );

        assertNotNull(result);
        assertEquals(4, result.getNetworkId());
        assertEquals(GeneralSmscConstants.SMPP_PROTOCOL, result.getProtocol());
        assertFalse(event.isSipMessageSupported());
        var listOfRetryDestNetworkId = Arrays.stream(event.getRetryDestNetworkId().split(",")).toList();
        assertEquals(2, listOfRetryDestNetworkId.size());
    }


    @Test
    @DisplayName("validate non destination when Only have SIP destination and SIP not supported")
    void validateAndGetNextNonSipDestinationWhenOnlyHaveSipDestinationAndSipIsNotSupportedThenReturnNoDestination() {
        RoutingRule.Destination currentDestination = new RoutingRule.Destination(1, 2, GeneralSmscConstants.SIP_PROTOCOL, "GW");
        RoutingRule.Destination secondDestination = new RoutingRule.Destination(2, 3, GeneralSmscConstants.SIP_PROTOCOL, "GW");
        MessageEvent event = MessageEvent.builder()
                .destinationAddr("50599999999")
                .retryDestNetworkId("")
                .build();

        RoutingRule.Destination result = routingMatcher.validateAndGetNextNonSipDestination(
                currentDestination,
                List.of(currentDestination, secondDestination),
                event,
                null
        );

        assertNull(result);
        var listOfRetryDestNetworkId = Arrays.stream(event.getRetryDestNetworkId().split(",")).toList();
        assertEquals(2, listOfRetryDestNetworkId.size());
    }

    @Test
    @DisplayName("validate registeredDelivery is set to 0 when origin is SMPP from Gateway and SIP is supported")
    void validateAndGetNextNonSipDestinationWhenOriginIsSmppGatewayThenRegisteredDeliveryIsZero() {
        RoutingRule.Destination currentDestination = new RoutingRule.Destination(1, 2, GeneralSmscConstants.SIP_PROTOCOL, "GW");
        MessageEvent event = MessageEvent.builder()
                .destinationAddr("50599999999")
                .retryDestNetworkId("")
                .destNetworkId(2)
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .originNetworkType("GW")
                .registeredDelivery(1)
                .build();
        MessageRegister registerData = MessageRegister.builder()
                .msisdn("50599999999")
                .aor("sip:user@domain")
                .build();

        RoutingRule.Destination result = routingMatcher.validateAndGetNextNonSipDestination(
                currentDestination,
                List.of(currentDestination),
                event,
                registerData
        );

        assertEquals(currentDestination, result);
        assertTrue(event.isSipMessageSupported());
        assertEquals(0, event.getRegisteredDelivery());
        assertEquals("sip:user@domain", event.getDestinationUri());
    }

    @Test
    @DisplayName("validate registeredDelivery is set to 0 when origin is HTTP from Gateway and SIP is supported")
    void validateAndGetNextNonSipDestinationWhenOriginIsHttpGatewayThenRegisteredDeliveryIsZero() {
        RoutingRule.Destination currentDestination = new RoutingRule.Destination(1, 2, GeneralSmscConstants.SIP_PROTOCOL, "GW");
        MessageEvent event = MessageEvent.builder()
                .destinationAddr("50599999999")
                .retryDestNetworkId("")
                .destNetworkId(2)
                .originProtocol(GeneralSmscConstants.HTTP_PROTOCOL)
                .originNetworkType("GW")
                .registeredDelivery(1)
                .build();
        MessageRegister registerData = MessageRegister.builder()
                .msisdn("50599999999")
                .aor("sip:user@domain")
                .build();

        RoutingRule.Destination result = routingMatcher.validateAndGetNextNonSipDestination(
                currentDestination,
                List.of(currentDestination),
                event,
                registerData
        );

        assertEquals(currentDestination, result);
        assertTrue(event.isSipMessageSupported());
        assertEquals(0, event.getRegisteredDelivery());
        assertEquals("sip:user@domain", event.getDestinationUri());
    }

    @Test
    @DisplayName("validate registeredDelivery is kept when origin is SMPP from Service Provider and SIP is supported")
    void validateAndGetNextNonSipDestinationWhenOriginIsSmppServiceProviderThenRegisteredDeliveryUnchanged() {
        RoutingRule.Destination currentDestination = new RoutingRule.Destination(1, 2, GeneralSmscConstants.SIP_PROTOCOL, "GW");
        MessageEvent event = MessageEvent.builder()
                .destinationAddr("50599999999")
                .retryDestNetworkId("")
                .destNetworkId(2)
                .originProtocol(GeneralSmscConstants.SMPP_PROTOCOL)
                .originNetworkType("SP")
                .registeredDelivery(1)
                .build();
        MessageRegister registerData = MessageRegister.builder()
                .msisdn("50599999999")
                .aor("sip:user@domain")
                .build();

        RoutingRule.Destination result = routingMatcher.validateAndGetNextNonSipDestination(
                currentDestination,
                List.of(currentDestination),
                event,
                registerData
        );

        assertEquals(currentDestination, result);
        assertTrue(event.isSipMessageSupported());
        assertEquals(1, event.getRegisteredDelivery());
        assertEquals("sip:user@domain", event.getDestinationUri());
    }

    @Test
    @DisplayName("getDestinationForAlternative when RetryDestNetworkId Is Null")
    void getDestinationForAlternativeWhenRetryDestNetworkIdIsNull() {
        RoutingRule routingRule = new RoutingRule();
        RoutingRule.Destination currentDestination = new RoutingRule.Destination(1, 2, GeneralSmscConstants.SIP_PROTOCOL, "GW");
        RoutingRule.Destination nextDestination = new RoutingRule.Destination(2, 3, GeneralSmscConstants.HTTP_PROTOCOL, "GW");

        routingRule.setDestination(List.of(currentDestination, nextDestination));
        MessageEvent event = MessageEvent.builder()
                .sourceAddr("50588888888")
                .destinationAddr("50599999999")
                .originNetworkId(1)
                .destNetworkId(2)
                .retryDestNetworkId(null)
                .build();

        RoutingRule.Destination result = routingMatcher.getDestinationForAlternative(routingRule, event);
        assertNotNull(result);
        assertEquals(2, result.getPriority());
        assertEquals(3, result.getNetworkId());
    }
}
