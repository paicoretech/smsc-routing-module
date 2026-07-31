package paicbd.smsc.routing.loaders;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import org.springframework.stereotype.Component;

import com.paicbd.smsc.dto.ServiceProvider;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;
import redis.clients.jedis.JedisCluster;

@Slf4j
@Component
@RequiredArgsConstructor
public class ServiceProvidersLoader {
    private final JedisCluster jedisCluster;
    private final ConcurrentMap<Integer, ServiceProvider> serviceProviders;

    @PostConstruct
    public void init() {
        this.loadServiceProviders();
    }

    private void loadServiceProviders() {
        Map<String, String> spRedisMap = jedisCluster.hgetAll(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME);
        if (spRedisMap.isEmpty()) {
            log.warn("No service providers found in cache.");
            return;
        }

        spRedisMap.forEach((networkId, spInRaw) -> {
            setValuesInMap(spInRaw);
            log.info("We has loaded service provider for network id: {}", networkId);
        });

        log.info("Finished loading service providers for {} networkIds", serviceProviders.size());
    }

    public void updateServiceProvider(String networkId) {
        String spInRaw = jedisCluster.hget(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME, networkId);
        if (Objects.isNull(spInRaw)) {
            log.warn("Service providers was not found on updateServiceProvider");
            return;
        }
        setValuesInMap(spInRaw);
        log.debug("Service provider for network id {} has been added or updated", networkId);
    }

    public void deleteServiceProvider(String networkId) {
        ServiceProvider toRemove = serviceProviders.remove(Integer.parseInt(networkId));
        if (Objects.nonNull(toRemove)) {
            log.info("Deleted service provider for network id: {}", networkId);
        } else {
            log.warn("The service provider with network id {} was not found", networkId);
        }
    }

    private void setValuesInMap(String spInRaw) {
        try {
            spInRaw = spInRaw.replace("\\", "\\\\");
            ServiceProvider serviceProvider = Converter.stringToObject(spInRaw, ServiceProvider.class);
            Assert.notNull(serviceProvider, "An error occurred while casting ServiceProvider");
            serviceProviders.put(serviceProvider.getNetworkId(), serviceProvider);
        } catch (Exception e) {
            log.error("Error setting values Service Providers Map: {}", e.getMessage());
        }
    }
}
