package paicbd.smsc.routing.config;

import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.ws.SocketClient;
import com.paicbd.smsc.ws.SocketSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import paicbd.smsc.routing.component.CustomFrameHandler;
import paicbd.smsc.routing.util.AppProperties;

import java.util.List;

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
import static paicbd.smsc.routing.util.Constants.UPDATE_SMPP_GATEWAY;
import static paicbd.smsc.routing.util.Constants.UPDATE_SS7_CONFIG;

@Slf4j
@Generated
@Configuration
@RequiredArgsConstructor
public class WebSocketConfig {
    private final AppProperties appProperties;
    private final SocketSession socketSession;
    private final CustomFrameHandler customFrameHandler;

    @Bean
    public SocketClient socketClient() {
        List<String> topicsToSubscribe = List.of(
                UPDATE_ROUTING_RULE,
                DELETE_ROUTING_RULE,
                UPDATE_GS_SMPP_HTTP,
                UPDATE_SS7_CONFIG,
                CONNECT_SS7_GATEWAY,
                DELETE_SS7_CONFIG,
                UPDATE_SMPP_GATEWAY,
                CONNECT_SMPP_GATEWAY,
                STOP_SMPP_GATEWAY,
                DELETE_SMPP_GATEWAY,
                UPDATE_SERVICE_SMPP_PROVIDER_ENDPOINT,
                DELETE_SERVICE_SMPP_PROVIDER_ENDPOINT,
                UPDATE_HTTP_GATEWAY,
                CONNECT_HTTP_GATEWAY,
                STOP_HTTP_GATEWAY,
                DELETE_HTTP_GATEWAY,
                UPDATE_SERVICE_HTTP_PROVIDER_ENDPOINT,
                DELETE_SERVICE_HTTP_PROVIDER_ENDPOINT,
                DELETE_SIP_GATEWAYS_ENDPOINT,
                CONNECT_SIP_GATEWAYS_ENDPOINT,
                STOP_SIP_GATEWAYS_ENDPOINT
        );
        UtilsRecords.WebSocketConnectionParams wsp = new UtilsRecords.WebSocketConnectionParams(
                appProperties.isWsEnabled(),
                appProperties.getWsHost(),
                appProperties.getWsPort(),
                appProperties.getWsPath(),
                topicsToSubscribe,
                appProperties.getWsHeaderName(),
                appProperties.getWsHeaderValue(),
                appProperties.getWsRetryInterval(),
                "ROUTING-MODULE"
        );
        return new SocketClient(customFrameHandler, wsp, socketSession);
    }
}
