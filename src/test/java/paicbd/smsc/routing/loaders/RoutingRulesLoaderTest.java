package paicbd.smsc.routing.loaders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paicbd.smsc.dto.RoutingRule;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import paicbd.smsc.routing.util.AppProperties;
import redis.clients.jedis.JedisCluster;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@ExtendWith(MockitoExtension.class)
class RoutingRulesLoaderTest {
    @Mock
    JedisCluster jedisCluster;

    @Mock
    AppProperties appProperties;

    @Mock
    ConcurrentMap<Integer, List<RoutingRule>> routingRules;

    @Spy
    @InjectMocks
    RoutingRulesLoader routingRulesLoader;

    @BeforeEach
    void setUp() {
        routingRules = spy(new ConcurrentHashMap<>());
        routingRulesLoader = new RoutingRulesLoader(jedisCluster, routingRules);
    }

    @Test
    @DisplayName("Init when not found data in redis then routing rules is empty")
    void initWhenNotFoundDataInRedisThenRoutingRulesIsEmpty() {
        when(jedisCluster.hgetAll(GeneralSmscConstants.ROUTING_RULES_HASH_NAME)).thenReturn(Collections.emptyMap());
        routingRulesLoader.init();
        assertEquals(0, routingRules.size());
    }

    @Test
    @DisplayName("Init when found data in redis then routing rules is not empty")
    void initWhenFoundDataInRedisThenRoutingRulesIsNotEmpty() throws JsonProcessingException {
        String routingRulesString = new ObjectMapper().writeValueAsString(getRoutingRules());
        when(jedisCluster.hgetAll(GeneralSmscConstants.ROUTING_RULES_HASH_NAME)).thenReturn(Collections.singletonMap("1", routingRulesString));
        routingRulesLoader.init();
        assertEquals(1, routingRules.size());
    }

    @Test
    @DisplayName("Init when redis returns invalid data then routing rules is empty")
    void initWhenRedisReturnsInvalidDataThenRoutingRulesIsEmpty() {
        when(jedisCluster.hgetAll(GeneralSmscConstants.ROUTING_RULES_HASH_NAME)).thenReturn(Collections.singletonMap("1", "invalid data"));
        assertThrows(NullPointerException.class, () -> routingRulesLoader.init());
    }

    @Test
    @DisplayName("Update routing rule when routing rule not found")
    void updateRoutingRuleWhenRoutingRuleNotFound() {
        when(jedisCluster.hget(GeneralSmscConstants.ROUTING_RULES_HASH_NAME, "1")).thenReturn(null);
        routingRulesLoader.updateRoutingRule("1");
        verify(jedisCluster).hget(GeneralSmscConstants.ROUTING_RULES_HASH_NAME, "1");
        verify(routingRules, never()).computeIfAbsent(1, k -> null);
    }

    @Test
    @DisplayName("Update routing rule when routing rule found")
    void updateRoutingRuleWhenRoutingRuleFound() throws JsonProcessingException {
        String routingRulesString = new ObjectMapper().writeValueAsString(getRoutingRules());
        when(jedisCluster.hget(GeneralSmscConstants.ROUTING_RULES_HASH_NAME, "1")).thenReturn(routingRulesString);
        routingRulesLoader.updateRoutingRule("1");
        verify(jedisCluster).hget(GeneralSmscConstants.ROUTING_RULES_HASH_NAME, "1");
        verify(routingRules).put(anyInt(), any());
    }

    @Test
    @DisplayName("Update routing rule when redis returns invalid data")
    void deleteRoutingRuleWhenRoutingRuleNotFound() {
        when(routingRules.remove(1)).thenReturn(null);
        routingRulesLoader.deleteRoutingRule("1");
        verify(routingRules).remove(1);
    }

    @Test
    @DisplayName("Delete routing rule when routing rule found")
    void deleteRoutingRuleWhenRoutingRuleFound() {
        when(routingRules.remove(1)).thenReturn(getRoutingRules());
        routingRulesLoader.deleteRoutingRule("1");
        verify(routingRules).remove(1);
    }

    static List<RoutingRule> getRoutingRules() {
        RoutingRule.Destination destination = new RoutingRule.Destination();
        destination.setNetworkId(2);
        destination.setPriority(1);
        destination.setProtocol("SMPP");
        destination.setNetworkType("GW");

        List<RoutingRule.Destination> destinations = List.of(destination);

        RoutingRule rr1 = RoutingRule.builder()
                .originNetworkId(1)
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
                .build();

        RoutingRule rr2 = RoutingRule.builder()
                .originNetworkId(1)
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
                .build();

        return List.of(rr1, rr2);
    }
}