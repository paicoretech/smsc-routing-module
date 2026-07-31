package paicbd.smsc.routing.component;

import com.paicbd.smsc.ws.FrameHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.stereotype.Component;
import paicbd.smsc.routing.loaders.GatewaysLoader;
import paicbd.smsc.routing.loaders.RoutingRulesLoader;
import paicbd.smsc.routing.loaders.ServiceProvidersLoader;
import paicbd.smsc.routing.loaders.SettingsLoader;

import java.util.Objects;

import static com.paicbd.smsc.utils.WebsocketConstants.ADD_OR_UPDATE_SMS_DIAMETER_GATEWAY;
import static com.paicbd.smsc.utils.WebsocketConstants.REMOVE_DIAMETER_GATEWAY;
import static paicbd.smsc.routing.util.Constants.CONNECT_HTTP_GATEWAY;
import static paicbd.smsc.routing.util.Constants.CONNECT_SIP_GATEWAYS_ENDPOINT;
import static paicbd.smsc.routing.util.Constants.CONNECT_SMPP_GATEWAY;
import static paicbd.smsc.routing.util.Constants.CONNECT_SS7_GATEWAY;
import static paicbd.smsc.routing.util.Constants.DELETE_HTTP_GATEWAY;
import static paicbd.smsc.routing.util.Constants.DELETE_ROUTING_RULE;
import static paicbd.smsc.routing.util.Constants.DELETE_SERVICE_HTTP_PROVIDER_ENDPOINT;
import static paicbd.smsc.routing.util.Constants.DELETE_SERVICE_SMPP_PROVIDER_ENDPOINT;
import static paicbd.smsc.routing.util.Constants.DELETE_SIP_GATEWAYS_ENDPOINT;
import static paicbd.smsc.routing.util.Constants.DELETE_SMPP_GATEWAY;
import static paicbd.smsc.routing.util.Constants.DELETE_SS7_CONFIG;
import static paicbd.smsc.routing.util.Constants.STOP_HTTP_GATEWAY;
import static paicbd.smsc.routing.util.Constants.STOP_SIP_GATEWAYS_ENDPOINT;
import static paicbd.smsc.routing.util.Constants.STOP_SMPP_GATEWAY;
import static paicbd.smsc.routing.util.Constants.UPDATE_GS_SMPP_HTTP;
import static paicbd.smsc.routing.util.Constants.UPDATE_HTTP_GATEWAY;
import static paicbd.smsc.routing.util.Constants.UPDATE_ROUTING_RULE;
import static paicbd.smsc.routing.util.Constants.UPDATE_SERVICE_HTTP_PROVIDER_ENDPOINT;
import static paicbd.smsc.routing.util.Constants.UPDATE_SERVICE_SMPP_PROVIDER_ENDPOINT;
import static paicbd.smsc.routing.util.Constants.UPDATE_SIP_GATEWAYS_ENDPOINT;
import static paicbd.smsc.routing.util.Constants.UPDATE_SMPP_GATEWAY;
import static paicbd.smsc.routing.util.Constants.UPDATE_SS7_CONFIG;

@Slf4j
@Component
@RequiredArgsConstructor
public class CustomFrameHandler implements FrameHandler {
    private final SettingsLoader settingsLoader;
    private final GatewaysLoader gatewaysLoader;
    private final CreditHandler creditHandler;
    private final RoutingRulesLoader routingRulesLoader;
    private final ServiceProvidersLoader serviceProvidersLoader;

    @Override
    public void handleFrameLogic(StompHeaders headers, Object payload) {
        String identifier = payload.toString();
        String destination = headers.getDestination();
        Objects.requireNonNull(identifier, "System ID cannot be null");
        Objects.requireNonNull(destination, "Destination cannot be null");

        switch (destination) {
            case UPDATE_ROUTING_RULE -> routingRulesLoader.updateRoutingRule(identifier);
            case DELETE_ROUTING_RULE -> routingRulesLoader.deleteRoutingRule(identifier);
            case UPDATE_GS_SMPP_HTTP -> {
                settingsLoader.loadOrUpdateSmppHttpSettings();
                settingsLoader.loadOrUpdateCommonSettings();
            }
            case UPDATE_SS7_CONFIG, CONNECT_SS7_GATEWAY -> settingsLoader.updateSpecificSs7Setting(Integer.parseInt(identifier));
            case DELETE_SS7_CONFIG -> settingsLoader.removeFromSs7Map(Integer.parseInt(identifier));
            case UPDATE_SMPP_GATEWAY, CONNECT_SMPP_GATEWAY, STOP_SMPP_GATEWAY, UPDATE_HTTP_GATEWAY,
                 CONNECT_HTTP_GATEWAY, STOP_HTTP_GATEWAY -> gatewaysLoader.updateGateway(identifier);
            case DELETE_SMPP_GATEWAY, DELETE_HTTP_GATEWAY -> gatewaysLoader.deleteGateway(identifier);
            case UPDATE_SERVICE_SMPP_PROVIDER_ENDPOINT, UPDATE_SERVICE_HTTP_PROVIDER_ENDPOINT ->
                    serviceProvidersLoader.updateServiceProvider(identifier);
            case DELETE_SERVICE_SMPP_PROVIDER_ENDPOINT, DELETE_SERVICE_HTTP_PROVIDER_ENDPOINT ->
                    deleteAction(identifier);
            case ADD_OR_UPDATE_SMS_DIAMETER_GATEWAY -> gatewaysLoader.updateDiameterGateway(identifier);
            case REMOVE_DIAMETER_GATEWAY -> gatewaysLoader.deleteDiameterGateway(identifier);
            case UPDATE_SIP_GATEWAYS_ENDPOINT, CONNECT_SIP_GATEWAYS_ENDPOINT, STOP_SIP_GATEWAYS_ENDPOINT -> gatewaysLoader.updateSipGateways(identifier);
            case DELETE_SIP_GATEWAYS_ENDPOINT -> gatewaysLoader.deleteSipGateways(identifier);
            default -> log.warn("Unknown destination: {}", headers.getDestination());
        }
    }

    private void deleteAction(String identifier) {
        serviceProvidersLoader.deleteServiceProvider(identifier);
        creditHandler.removeCreditUsed(identifier);
    }
}
