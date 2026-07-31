package paicbd.smsc.routing.component;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.stomp.StompHeaders;
import paicbd.smsc.routing.loaders.GatewaysLoader;
import paicbd.smsc.routing.loaders.RoutingRulesLoader;
import paicbd.smsc.routing.loaders.ServiceProvidersLoader;
import paicbd.smsc.routing.loaders.SettingsLoader;

import org.junit.jupiter.params.provider.Arguments;

import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static paicbd.smsc.routing.util.Constants.CONNECT_SS7_GATEWAY;
import static paicbd.smsc.routing.util.Constants.DELETE_ROUTING_RULE;
import static paicbd.smsc.routing.util.Constants.DELETE_SERVICE_SMPP_PROVIDER_ENDPOINT;
import static paicbd.smsc.routing.util.Constants.DELETE_SMPP_GATEWAY;
import static paicbd.smsc.routing.util.Constants.DELETE_SS7_CONFIG;
import static paicbd.smsc.routing.util.Constants.UPDATE_GS_SMPP_HTTP;
import static paicbd.smsc.routing.util.Constants.UPDATE_ROUTING_RULE;
import static paicbd.smsc.routing.util.Constants.UPDATE_SERVICE_SMPP_PROVIDER_ENDPOINT;
import static paicbd.smsc.routing.util.Constants.UPDATE_SMPP_GATEWAY;

@ExtendWith(MockitoExtension.class)
class CustomFrameHandlerTest {

    @Mock
    private SettingsLoader settingsLoader;

    @Mock
    private GatewaysLoader gatewaysLoader;

    @Mock
    private CreditHandler creditHandler;

    @Mock
    private RoutingRulesLoader routingRulesLoader;

    @Mock
    private ServiceProvidersLoader serviceProvidersLoader;

    @InjectMocks
    private CustomFrameHandler customFrameHandler;

    @ParameterizedTest
    @MethodSource("provideTestCases")
    void testHandleFrameLogic(String destination, String payload, Consumer<CustomFrameHandlerTest> verification) {
        StompHeaders headers = new StompHeaders();
        headers.setDestination(destination);

        assertDoesNotThrow(() -> customFrameHandler.handleFrameLogic(headers, payload));

        verification.accept(this);
    }

    private static Stream<Arguments> provideTestCases() {
        return Stream.of(
                // Test case for UPDATE_ROUTING_RULE
                Arguments.of(
                        UPDATE_ROUTING_RULE, "1",
                        (Consumer<CustomFrameHandlerTest>) test ->
                                verify(test.routingRulesLoader).updateRoutingRule("1")),
                // Test case for DELETE_ROUTING_RULE
                Arguments.of(
                        DELETE_ROUTING_RULE, "1",
                        (Consumer<CustomFrameHandlerTest>) test ->
                                verify(test.routingRulesLoader).deleteRoutingRule("1")),
                // Test case for UPDATE_GS_SMPP_HTTP
                Arguments.of(
                        UPDATE_GS_SMPP_HTTP, "1",
                        (Consumer<CustomFrameHandlerTest>) test ->
                                verify(test.settingsLoader).loadOrUpdateSmppHttpSettings()),
                Arguments.of(
                        UPDATE_GS_SMPP_HTTP, "1",
                        (Consumer<CustomFrameHandlerTest>) test ->
                                verify(test.settingsLoader).loadOrUpdateCommonSettings()),
                // Test case for DELETE_SS7_CONFIG
                Arguments.of(
                        DELETE_SS7_CONFIG, "1",
                        (Consumer<CustomFrameHandlerTest>) test ->
                                verify(test.settingsLoader).removeFromSs7Map(1)),
                // Test case for DELETE_SERVICE_SMPP_PROVIDER_ENDPOINT
                Arguments.of(
                        DELETE_SERVICE_SMPP_PROVIDER_ENDPOINT, "1",
                        (Consumer<CustomFrameHandlerTest>) test -> {
                            verify(test.serviceProvidersLoader).deleteServiceProvider("1");
                            verify(test.creditHandler).removeCreditUsed("1");
                        }),
                // Test case for UPDATE_GATEWAY
                Arguments.of(
                        UPDATE_SMPP_GATEWAY, "1",
                        (Consumer<CustomFrameHandlerTest>) test ->
                                verify(test.gatewaysLoader).updateGateway("1")),
                // Test case for DELETE_GATEWAY
                Arguments.of(
                        DELETE_SMPP_GATEWAY, "1",
                        (Consumer<CustomFrameHandlerTest>) test ->
                                verify(test.gatewaysLoader).deleteGateway("1")),
                // Test case for UPDATE_SERVICE_SMPP_PROVIDER_ENDPOINT
                Arguments.of(
                        UPDATE_SERVICE_SMPP_PROVIDER_ENDPOINT, "1",
                        (Consumer<CustomFrameHandlerTest>) test ->
                                verify(test.serviceProvidersLoader).updateServiceProvider("1")),
                // Test case for CONNECT_SS7_GATEWAY
                Arguments.of(
                        CONNECT_SS7_GATEWAY, "1",
                        (Consumer<CustomFrameHandlerTest>) test ->
                                verify(test.settingsLoader).updateSpecificSs7Setting(1)),
                // Test case for UNKNOWN_DESTINATION
                Arguments.of(
                        "INVALID_DESTINATION", "1",
                        (Consumer<CustomFrameHandlerTest>) test ->
                                verifyNoInteractions(test.settingsLoader, test.gatewaysLoader, test.creditHandler, test.routingRulesLoader, test.serviceProvidersLoader))
        );
    }
}
