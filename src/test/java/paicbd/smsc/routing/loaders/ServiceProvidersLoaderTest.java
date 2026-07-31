package paicbd.smsc.routing.loaders;

import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServiceProvidersLoaderTest {
    @Mock
    JedisCluster jedisCluster;

    @Mock
    ConcurrentMap<Integer, ServiceProvider> serviceProviders;

    @Spy
    @InjectMocks
    ServiceProvidersLoader serviceProvidersLoader;

    @BeforeEach
    void setUp() {
        serviceProviders = spy(new ConcurrentHashMap<>());
        serviceProvidersLoader = new ServiceProvidersLoader(jedisCluster, serviceProviders);
    }

    @Test
    @DisplayName("Init without data in redis then service providers is empty")
    void initWithoutDataInRedisThenServiceProvidersIsEmpty() {
        serviceProvidersLoader.init();
        assertEquals(0, serviceProviders.size());
    }

    @Test
    @DisplayName("Init with data in redis then service providers is not empty")
    void initWithDataInRedisThenServiceProvidersIsNotEmpty() {
        ServiceProvider serviceProvider = ServiceProvider.builder()
                .networkId(1)
                .name("fakeServiceProvider")
                .build();
        when(jedisCluster.hgetAll(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME)).thenReturn(Map.of("1", serviceProvider.toString()));
        serviceProvidersLoader.init();
        assertEquals(1, serviceProviders.size());
    }

    @Test
    @DisplayName("Init with data in redis but invalid then service providers is empty")
    void initWithDataInRedisButInvalidThenServiceProvidersIsEmpty() {
        when(jedisCluster.hgetAll(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME)).thenReturn(Map.of("1", "invalid"));
        serviceProvidersLoader.init();
        assertEquals(0, serviceProviders.size());
    }

    @Test
    @DisplayName("Update Service Provider when redis return null then not set value in Map")
    void updateServiceProviderWhenRedisReturnNullThenNotSetValueInMap() {
        when(jedisCluster.hget(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME, "1")).thenReturn(null);
        serviceProvidersLoader.updateServiceProvider("1");
        verify(serviceProviders, never()).put(anyInt(), any(ServiceProvider.class));
    }

    @Test
    @DisplayName("Update Service Provider when redis return valid data then set value in Map")
    void updateServiceProviderWhenRedisReturnValidDataThenSetValueInMap() {
        ServiceProvider serviceProvider = ServiceProvider.builder()
                .networkId(1)
                .name("fakeServiceProvider")
                .binds(new ArrayList<>())
                .tps(new AtomicInteger(0))
                .creditUsed(new AtomicLong(0L))
                .validity(0)
                .headerSecurityName("Authorization")
                .callbackHeadersHttp(new ArrayList<>())
                .build();
        when(jedisCluster.hget(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME, "1")).thenReturn(serviceProvider.toString());
        serviceProvidersLoader.updateServiceProvider("1");

        ArgumentCaptor<Integer> networkIdCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<ServiceProvider> serviceProviderCaptor = ArgumentCaptor.forClass(ServiceProvider.class);

        verify(serviceProviders).put(networkIdCaptor.capture(), serviceProviderCaptor.capture());

        assertEquals(1, serviceProviders.size());
        assertEquals(1, (int) networkIdCaptor.getValue());
        assertEquals(serviceProvider.toString(), serviceProviderCaptor.getValue().toString());
    }

    @Test
    @DisplayName("Delete Service Provider when not found")
    void deleteServiceProviderWhenNotFound() {
        ServiceProvider serviceProvider = ServiceProvider.builder()
                .networkId(1)
                .name("fakeServiceProvider")
                .build();
        serviceProviders.put(1, serviceProvider);
        serviceProvidersLoader = new ServiceProvidersLoader(jedisCluster, serviceProviders);

        serviceProvidersLoader.deleteServiceProvider("1");
        verify(serviceProviders).remove(1);
        assertEquals(0, serviceProviders.size());
    }

    @Test
    @DisplayName("Delete Service Provider when found")
    void deleteServiceProviderWhenFound() {
        ServiceProvider serviceProvider = ServiceProvider.builder()
                .networkId(1)
                .name("fakeServiceProvider")
                .build();
        serviceProviders.put(1, serviceProvider);
        serviceProvidersLoader = new ServiceProvidersLoader(jedisCluster, serviceProviders);

        serviceProvidersLoader.deleteServiceProvider("2");
        verify(serviceProviders).remove(2);
        assertEquals(1, serviceProviders.size());
    }
}