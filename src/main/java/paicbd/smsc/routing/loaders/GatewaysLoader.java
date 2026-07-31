package paicbd.smsc.routing.loaders;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.paicbd.smsc.dto.SipGateways;
import com.paicbd.smsc.dto.diameter.DiameterConfig;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import org.springframework.stereotype.Component;

import com.paicbd.smsc.dto.Gateway;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;
import redis.clients.jedis.JedisCluster;

@Slf4j
@Component
@RequiredArgsConstructor
public class GatewaysLoader {
    private final JedisCluster jedisCluster;
    private final ConcurrentMap<Integer, Gateway> gateways;
    private final ConcurrentMap<Integer, DiameterConfig> diameterGatewayMap;
    private final ConcurrentMap<Integer, SipGateways> sipGatewaysConcurrentMap;

    @PostConstruct
    public void init() {
        this.loadGateways();
        this.loadDiameterGateways();
        this.loadSipGateways();
    }

    private void loadGateways() {
        Map<String, String> gatewaysRedisMap = jedisCluster.hgetAll(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME);
        if (gatewaysRedisMap.isEmpty()) {
            log.warn("No gateways found in cache.");
            return;
        }

        gatewaysRedisMap.forEach((networkId, gatewayInRaw) -> {
            Gateway gateway = Converter.stringToObject(gatewayInRaw, Gateway.class);
            Assert.notNull(gateway, "An error occurred while casting Gateway in loadGateways");
            gateways.put(gateway.getNetworkId(), gateway);
        });

        log.info("Finished loading gateways for {} networkIds", gateways.size());
    }

    private void loadDiameterGateways() {
        Map<String, String> diameterConfigMap = jedisCluster.hgetAll(GeneralSmscConstants.DIAMETER_GATEWAYS_HASH_NAME);
        if (diameterConfigMap.isEmpty()) {
            log.warn("No diameter configuration for gateways found in Redis");
            return;
        }

        diameterConfigMap.forEach((id, diameterConfigInRaw) -> {
           try {
               DiameterConfig diameterConfig =  Converter.stringToObject(diameterConfigInRaw, DiameterConfig.class);
               Assert.notNull(diameterConfig, "An error occurred while casting DiameterConfig in loadGateways");
               diameterGatewayMap.put(diameterConfig.getNetworkId(), diameterConfig);
           } catch (Exception e) {
               log.error("Error while casting DiameterConfig in loadGateways for id {}", id, e);
           }
        });
        log.info("Finished loading Diameter gateways for {} networkIds", diameterGatewayMap.size());
    }

    private void loadSipGateways() {
        Map<String, String> sipGatewaysMapFromRedis = jedisCluster.hgetAll(GeneralSmscConstants.SIP_GATEWAYS_HASH_NAME);
        if (sipGatewaysMapFromRedis.isEmpty()) {
            log.warn("No sip gateways found for connect");
            return;
        }

        sipGatewaysMapFromRedis.forEach((id, sipGatewaysInRaw) -> {
            try {
                SipGateways sipGateways =  Converter.stringToObject(sipGatewaysInRaw, SipGateways.class);
                Assert.notNull(sipGateways, "An error occurred while casting sip gateway in loadSipGateways");
                sipGatewaysConcurrentMap.put(sipGateways.getNetworkId(), sipGateways);
            } catch (Exception e) {
                log.error("Error while casting sip gateways in loadSipGateways for id {}", id, e);
            }
        });
        log.info("Finished loading sip gateways for {} networkIds", sipGatewaysConcurrentMap.size());
    }

    public void updateGateway(String networkId) {
        Assert.notNull(networkId, "NetworkId is null on updateGateway");
        String gatewayInRaw = jedisCluster.hget(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME, networkId);
        if (gatewayInRaw == null) {
            log.warn("Gateways was not found on updateGateway");
            return;
        }
        Gateway gateway = Converter.stringToObject(gatewayInRaw, Gateway.class);
        Assert.notNull(gateway, "An error occurred while casting Gateway in updateGateway");

        gateways.put(gateway.getNetworkId(), gateway);
        log.info("The gateway has been updated successfully. NetworkId {}: Gateway {}", networkId, gateway);
    }

    public void deleteGateway(String networkId) {
        Assert.notNull(networkId, "NetworkId is null on deleteGateway");
        gateways.remove(Integer.parseInt(networkId));
        log.info("The gateway has been deleted successfully. NetworkId {}", networkId);
    }

    public void updateSipGateways(String networkId) {
        Assert.notNull(networkId, "NetworkId is null on updateSipGateways");
        String sipGatewaysInRaw = jedisCluster.hget(GeneralSmscConstants.SIP_GATEWAYS_HASH_NAME, networkId);
        if (sipGatewaysInRaw == null) {
            log.warn("SIP gateway was not found on updateGateway");
            return;
        }
        SipGateways sipGateways = Converter.stringToObject(sipGatewaysInRaw, SipGateways.class);
        Assert.notNull(sipGateways, "An error occurred while casting SIP gateways in sipGateways");

        sipGatewaysConcurrentMap.put(sipGateways.getNetworkId(), sipGateways);
        log.info("The gateway has been updated successfully. NetworkId {}: SIP gateways {}", networkId, sipGateways);
    }

    public void deleteSipGateways(String networkId) {
        Assert.notNull(networkId, "NetworkId is null on deleteSipGateways");
        sipGatewaysConcurrentMap.remove(Integer.parseInt(networkId));
        log.info("The SIP gateways has been deleted successfully. NetworkId {}", networkId);
    }

    public void updateDiameterGateway(String id) {
        Assert.notNull(id, "Id is null on updateDiameterGateway");
        String diameterGatewayInRaw = jedisCluster.hget(GeneralSmscConstants.DIAMETER_GATEWAYS_HASH_NAME, id);
        if (diameterGatewayInRaw == null) {
            log.warn("Diameter Gateway was not found on updateGateway");
            return;
        }
        DiameterConfig diameterConfig =  Converter.stringToObject(diameterGatewayInRaw, DiameterConfig.class);
        Assert.notNull(diameterConfig, "An error occurred while casting DiameterConfig in updateDiameterGateway");
        diameterGatewayMap.put(diameterConfig.getNetworkId(), diameterConfig);
        log.info("The Diameter gateway has been updated successfully. NetworkId {}: Gateway {}", diameterConfig.getNetworkId(), diameterConfig);
    }

    public void deleteDiameterGateway(String id) {
        Assert.notNull(id, "Id is null on deleteDiameterGateway");
        diameterGatewayMap.entrySet().removeIf(entry -> entry.getValue().getId().toString().equals(id));
        log.info("The Diameter gateway has been deleted successfully. Id {}", id);
    }


}
