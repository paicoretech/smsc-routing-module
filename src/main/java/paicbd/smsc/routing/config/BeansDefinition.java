package paicbd.smsc.routing.config;

import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.RoutingRule;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.SipGateways;
import com.paicbd.smsc.dto.Ss7Settings;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.dto.diameter.DiameterConfig;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.ws.SocketSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import paicbd.smsc.routing.util.AppProperties;
import redis.clients.jedis.JedisCluster;

import java.net.http.HttpClient;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Generated
@Configuration
@RequiredArgsConstructor
public class BeansDefinition {
    private final AppProperties appProperties;

    @Bean
    public ConcurrentMap<Integer, List<RoutingRule>> routingRules() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<Integer, Ss7Settings> ss7SettingsMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<Integer, Gateway> gateways() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<Integer, DiameterConfig> diameterGatewayMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<Integer, SipGateways> sipGatewaysMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<Integer, ServiceProvider> serviceProviders() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<Integer, AtomicLong> creditUsed() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public SocketSession socketSession() {
        return new SocketSession("routing");
    }

    @Bean
    public HttpClient httpClient() {
        return HttpClient.newHttpClient();
    }

    @Bean
    public JedisCluster jedisCluster() {
        return Converter.paramsToJedisCluster(
                new UtilsRecords.JedisConfigParams(appProperties.getRedisNodes(), appProperties.getRedisMaxTotal(),
                        appProperties.getRedisMinIdle(), appProperties.getRedisMaxIdle(),
                        appProperties.isRedisBlockWhenExhausted(), appProperties.getRedisConnectionTimeout(),
                        appProperties.getRedisSoTimeout(), appProperties.getRedisMaxAttempts(),
                        appProperties.getRedisUser(), appProperties.getRedisPassword())
        );
    }

    @Bean
    public ScyllaManager scyllaManager() {
        return new ScyllaManager(
                appProperties.getScyllaContactPoints(),
                appProperties.getScyllaDatacenter(),
                appProperties.getScyllaUser(),
                appProperties.getScyllaPassword(),
                false
        );
    }

    @Bean
    public ConcurrentMap<String, String> commonSettingsMap() {
        return new ConcurrentHashMap<>();
    }
}
