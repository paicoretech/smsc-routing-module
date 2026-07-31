package paicbd.smsc.routing.loaders;

import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.SipGateways;
import com.paicbd.smsc.dto.diameter.ConnectionType;
import com.paicbd.smsc.dto.diameter.DiameterConfig;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.DisplayName;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.JedisCluster;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GatewaysLoaderTest {
    @Mock
    JedisCluster jedisCluster;

    @Mock
    ConcurrentMap<Integer, Gateway> gateways;

    @Mock
    ConcurrentMap<Integer, DiameterConfig> diameterGatewayMap;

    @Mock
    ConcurrentMap<Integer, SipGateways> sipGatewaysConcurrentMap;

    @Spy
    @InjectMocks
    GatewaysLoader gatewaysLoader;

    @BeforeEach
    void setUp() {
        gateways = spy(new ConcurrentHashMap<>());
        diameterGatewayMap = spy(new ConcurrentHashMap<>());
        sipGatewaysConcurrentMap = spy(new ConcurrentHashMap<>());
        gatewaysLoader = new GatewaysLoader(jedisCluster, gateways, diameterGatewayMap, sipGatewaysConcurrentMap);
    }

    @Test
    @DisplayName("init loads data from redis")
    void initWithData() {
        Gateway gateway = Gateway.builder()
                .networkId(1)
                .name("fakeGateway")
                .ip("127.0.0.1")
                .port(8080)
                .systemId("systemId")
                .password("password")
                .build();


        DiameterConfig diameterConfig = new DiameterConfig();
        diameterConfig.setId(1);
        diameterConfig.setNetworkId(1);
        diameterConfig.setName("diameterConfig");
        diameterConfig.setEnabled(true);
        diameterConfig.setParameters(null);
        diameterConfig.setExtensionMode(ConnectionType.SCTP);

        Map<String, String> gatewaysMap = Map.of("1", gateway.toString());
        Map<String, String> diametergatewaysMap = Map.of("1", diameterConfig.toString());
        SipGateways sipGateways = SipGateways.builder()
                .networkId(1)
                .name("sipGateways")
                .build();
        Map<String, String> sipGatewaysFromRedis = Map.of("1", sipGateways.toString());

        when(jedisCluster.hgetAll(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME)).thenReturn(gatewaysMap);
        when(jedisCluster.hgetAll(GeneralSmscConstants.DIAMETER_GATEWAYS_HASH_NAME)).thenReturn(diametergatewaysMap);
        when(jedisCluster.hgetAll(GeneralSmscConstants.SIP_GATEWAYS_HASH_NAME)).thenReturn(sipGatewaysFromRedis);

        gatewaysLoader.init();

        ArgumentCaptor<Integer> networkIdCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Gateway> gatewayCaptor = ArgumentCaptor.forClass(Gateway.class);
        verify(gateways).put(networkIdCaptor.capture(), gatewayCaptor.capture());
        assertEquals(1, gateways.size());
        assertEquals(1, (int) networkIdCaptor.getValue());
        Gateway capturedGateway = gatewayCaptor.getValue();
        assertEquals(gateway.getNetworkId(), capturedGateway.getNetworkId());
        assertEquals(gateway.getName(), capturedGateway.getName());
        assertEquals(gateway.getIp(), capturedGateway.getIp());
        assertEquals(gateway.getPort(), capturedGateway.getPort());
        assertEquals(gateway.getSystemId(), capturedGateway.getSystemId());
        assertEquals(gateway.getPassword(), capturedGateway.getPassword());

        ArgumentCaptor<Integer> idCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<DiameterConfig> diameterGatewayCaptor = ArgumentCaptor.forClass(DiameterConfig.class);
        verify(diameterGatewayMap).put(idCaptor.capture(), diameterGatewayCaptor.capture());
        assertEquals(1, idCaptor.getValue());
        assertEquals(1, diameterGatewayMap.size());

        ArgumentCaptor<Integer> sipNetworkIdCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<SipGateways> sipGatewaysCaptor = ArgumentCaptor.forClass(SipGateways.class);
        verify(sipGatewaysConcurrentMap).put(sipNetworkIdCaptor.capture(), sipGatewaysCaptor.capture());
        assertEquals(1, (int) sipNetworkIdCaptor.getValue());
        assertEquals(1, sipGatewaysConcurrentMap.size());
    }

    @Test
    @DisplayName("init handles empty data")
    void initWithoutData() {
        when(jedisCluster.hgetAll(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME)).thenReturn(Collections.emptyMap());
        when(jedisCluster.hgetAll(GeneralSmscConstants.DIAMETER_GATEWAYS_HASH_NAME)).thenReturn(Collections.emptyMap());
        when(jedisCluster.hgetAll(GeneralSmscConstants.SIP_GATEWAYS_HASH_NAME)).thenReturn(Collections.emptyMap());

        gatewaysLoader.init();
        assertTrue(gateways.isEmpty());
        assertTrue(diameterGatewayMap.isEmpty());
        assertTrue(sipGatewaysConcurrentMap.isEmpty());
    }

    @Test
    @DisplayName("update gateway with redis data")
    void updateGatewayWithRedisData() {
        Gateway currentGateway = Gateway.builder()
                .networkId(1)
                .name("fakeGateway")
                .ip("127.0.0.1")
                .port(8080)
                .systemId("systemId")
                .password("password")
                .build();

        Gateway updatedGateway = Gateway.builder()
                .networkId(1)
                .name("updatedGateway")
                .ip("127.0.0.2")
                .port(8081)
                .systemId("systemId1")
                .password("password1")
                .build();

        gateways.put(currentGateway.getNetworkId(), currentGateway);

        when(jedisCluster.hget(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME, "1")).thenReturn(updatedGateway.toString());

        gatewaysLoader.updateGateway("1");

        ArgumentCaptor<Integer> networkIdCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Gateway> gatewayCaptor = ArgumentCaptor.forClass(Gateway.class);

        verify(gateways, atLeast(1)).put(networkIdCaptor.capture(), gatewayCaptor.capture());

        assertEquals(1, gateways.size());
        assertEquals(1, (int) networkIdCaptor.getValue());

        Gateway capturedGateway = gatewayCaptor.getValue();
        assertEquals(updatedGateway.getNetworkId(), capturedGateway.getNetworkId());
        assertEquals(updatedGateway.getName(), capturedGateway.getName());
        assertEquals(updatedGateway.getIp(), capturedGateway.getIp());
        assertEquals(updatedGateway.getPort(), capturedGateway.getPort());
        assertEquals(updatedGateway.getSystemId(), capturedGateway.getSystemId());
        assertEquals(updatedGateway.getPassword(), capturedGateway.getPassword());
    }

    @Test
    @DisplayName("update gateway without data")
    void updateGatewayWithoutData() {
        when(jedisCluster.hget(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME, "1")).thenReturn(null);

        gatewaysLoader.updateGateway("1");

        assertTrue(gateways.isEmpty());
    }

    @Test
    @DisplayName("delete gateway")
    void deleteGateway() {
        Gateway gateway = Gateway.builder()
                .networkId(1)
                .name("fakeGateway")
                .ip("127.0.0.1")
                .port(8080)
                .systemId("systemId")
                .password("password")
                .build();

        gateways.put(gateway.getNetworkId(), gateway);
        gatewaysLoader.deleteGateway("1");
        verify(gateways).remove(1);
        assertTrue(gateways.isEmpty());
    }

    @Test
    @DisplayName("update sip settings with redis data")
    void updateSipGatewaysWithRedisData() {
        SipGateways currentSipGateways = SipGateways.builder()
                .networkId(1)
                .name("sipGateways")
                .build();
        SipGateways updatedSipGateways = SipGateways.builder()
                .networkId(1)
                .name("updatedSipGateways")
                .build();

        sipGatewaysConcurrentMap.put(currentSipGateways.getNetworkId(), currentSipGateways);
        when(jedisCluster.hget(GeneralSmscConstants.SIP_GATEWAYS_HASH_NAME, "1")).thenReturn(updatedSipGateways.toString());

        gatewaysLoader.updateSipGateways("1");

        ArgumentCaptor<Integer> networkIdCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<SipGateways> sipGatewaysCaptor = ArgumentCaptor.forClass(SipGateways.class);
        verify(sipGatewaysConcurrentMap, atLeast(1)).put(networkIdCaptor.capture(), sipGatewaysCaptor.capture());

        assertEquals(1, sipGatewaysConcurrentMap.size());
        assertEquals(1, (int) networkIdCaptor.getValue());
        assertEquals(updatedSipGateways.getNetworkId(), sipGatewaysCaptor.getValue().getNetworkId());
        assertEquals(updatedSipGateways.getName(), sipGatewaysCaptor.getValue().getName());
    }

    @Test
    @DisplayName("delete sip gateways")
    void deleteSipGateways() {
        SipGateways sipSettings = SipGateways.builder()
                .networkId(1)
                .name("sipGateways")
                .build();

        sipGatewaysConcurrentMap.put(sipSettings.getNetworkId(), sipSettings);
        gatewaysLoader.deleteSipGateways("1");
        verify(sipGatewaysConcurrentMap).remove(1);
        assertTrue(sipGatewaysConcurrentMap.isEmpty());
    }

    @Test
    @DisplayName("update diameter gateway when data exists")
    void updateDiameterGatewayWhenExistDataThenUpdateTheMap() {
        DiameterConfig currentDiameterGateway = new DiameterConfig();
        currentDiameterGateway.setId(1);
        currentDiameterGateway.setNetworkId(1);
        currentDiameterGateway.setName("diameterConfig");
        currentDiameterGateway.setEnabled(true);
        currentDiameterGateway.setMnoId(1);
        currentDiameterGateway.setGlobalTitle("50588888888");
        currentDiameterGateway.setExtensionMode(ConnectionType.SCTP);


        DiameterConfig updatedDiameterGateway = new DiameterConfig();
        updatedDiameterGateway.setId(1);
        updatedDiameterGateway.setNetworkId(1);
        updatedDiameterGateway.setName("diameterConfig");
        updatedDiameterGateway.setEnabled(true);
        updatedDiameterGateway.setMnoId(10);
        updatedDiameterGateway.setGlobalTitle("5057777777");
        updatedDiameterGateway.setExtensionMode(ConnectionType.SCTP);


        diameterGatewayMap.put(currentDiameterGateway.getNetworkId(), currentDiameterGateway);
        when(jedisCluster.hget(GeneralSmscConstants.DIAMETER_GATEWAYS_HASH_NAME, "1")).thenReturn(updatedDiameterGateway.toString());
        gatewaysLoader.updateDiameterGateway("1");

        ArgumentCaptor<Integer> networkIdCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<DiameterConfig> gatewayCaptor = ArgumentCaptor.forClass(DiameterConfig.class);

        verify(diameterGatewayMap, atLeast(1)).put(networkIdCaptor.capture(), gatewayCaptor.capture());

        assertEquals(1, diameterGatewayMap.size());
        assertEquals(1, (int) networkIdCaptor.getValue());

        DiameterConfig capturedGateway = gatewayCaptor.getValue();


        assertEquals(updatedDiameterGateway.getNetworkId(), capturedGateway.getNetworkId());
        assertEquals(updatedDiameterGateway.getName(), capturedGateway.getName());
        assertEquals(updatedDiameterGateway.getMnoId(), capturedGateway.getMnoId());
        assertEquals(updatedDiameterGateway.getGlobalTitle(), capturedGateway.getGlobalTitle());
    }

    @Test
    @DisplayName("delete diameter gateway")
    void deleteDiameterGateway() {
        DiameterConfig currentDiameterGateway = new DiameterConfig();
        currentDiameterGateway.setId(4);
        currentDiameterGateway.setNetworkId(1);
        currentDiameterGateway.setName("diameterConfig");
        currentDiameterGateway.setEnabled(true);
        currentDiameterGateway.setMnoId(1);
        currentDiameterGateway.setGlobalTitle("50588888888");
        currentDiameterGateway.setExtensionMode(ConnectionType.SCTP);

        diameterGatewayMap.put(currentDiameterGateway.getNetworkId(), currentDiameterGateway);
        assertEquals(1, diameterGatewayMap.size());
        gatewaysLoader.deleteDiameterGateway("4");
        assertTrue(diameterGatewayMap.isEmpty());
    }

}
