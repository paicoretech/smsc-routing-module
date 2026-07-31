package paicbd.smsc.routing.util;

import com.paicbd.smsc.utils.Generated;
import lombok.experimental.UtilityClass;

@Generated
@UtilityClass
public class Constants {
    public static final String UPDATE_ROUTING_RULE = "/app/update/routingRules";
    public static final String DELETE_ROUTING_RULE = "/app/delete/routingRules";
    public static final String UPDATE_GS_SMPP_HTTP = "/app/generalSettings";
    public static final String UPDATE_SS7_CONFIG = "/app/ss7/updateGateway";
    public static final String CONNECT_SS7_GATEWAY = "/app/ss7/connectGateway";
    public static final String DELETE_SS7_CONFIG = "/app/ss7/deleteGateway";
	public static final String UPDATE_SMPP_GATEWAY = "/app/updateGateway";
    public static final String CONNECT_SMPP_GATEWAY = "/app/connectGateway";
    public static final String STOP_SMPP_GATEWAY = "/app/stopGateway";
    public static final String DELETE_SMPP_GATEWAY = "/app/deleteGateway";
    public static final String UPDATE_HTTP_GATEWAY = "/app/http/updateGateway";
    public static final String CONNECT_HTTP_GATEWAY = "/app/http/connectGateway";
    public static final String STOP_HTTP_GATEWAY = "/app/http/stopGateway";
    public static final String DELETE_HTTP_GATEWAY = "/app/http/deleteGateway";
    public static final String DELETE_SERVICE_SMPP_PROVIDER_ENDPOINT = "/app/smpp/serviceProviderDeleted";
    public static final String UPDATE_SERVICE_SMPP_PROVIDER_ENDPOINT = "/app/smpp/updateServiceProvider";
	public static final String DELETE_SERVICE_HTTP_PROVIDER_ENDPOINT = "/app/http/serviceProviderDeleted";
	public static final String UPDATE_SERVICE_HTTP_PROVIDER_ENDPOINT = "/app/http/updateServiceProvider";
    public static final String DELETE_SIP_GATEWAYS_ENDPOINT = "/app/sip/deleteGateway";
    public static final String UPDATE_SIP_GATEWAYS_ENDPOINT = "/app/sip/updateGateway";
    public static final String CONNECT_SIP_GATEWAYS_ENDPOINT = "/app/sip/connectGateway";
    public static final String STOP_SIP_GATEWAYS_ENDPOINT = "/app/sip/stopGateway";
}
