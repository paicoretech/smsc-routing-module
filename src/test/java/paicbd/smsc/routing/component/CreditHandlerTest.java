package paicbd.smsc.routing.component;

import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import paicbd.smsc.routing.util.AppProperties;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CreditHandlerTest {
    @Mock
    private AppProperties appProperties;

    @Mock
    private HttpClient httpClient;

    @Spy
    @InjectMocks
    private CreditHandler creditHandler;

    private ConcurrentMap<Integer, AtomicLong> creditUsed;

    @BeforeEach
    void setUp() {
        creditUsed = new ConcurrentHashMap<>();
        creditHandler = new CreditHandler(httpClient, appProperties, creditUsed);
    }

    private void setupMockProperties(String url, String apiKey) {
        when(appProperties.getBackendUrl()).thenReturn(url);
        when(appProperties.getInstanceName()).thenReturn("instance");
        when(appProperties.getBackendApiKey()).thenReturn(apiKey);
    }

    private void assertRequestHeaders(HttpRequest capturedRequest) {
        assertEquals(MediaType.APPLICATION_JSON_VALUE, capturedRequest.headers().firstValue(HttpHeaders.CONTENT_TYPE).orElse(""));
        assertEquals("test-api-key", capturedRequest.headers().firstValue("X-API-Key").orElse(""));
    }

    @Test
    void testIncrementCreditUsed_NewNetworkId() {
        creditHandler.incrementCreditUsed(1, 100);
        assertEquals(100, creditUsed.get(1).get());
    }

    @Test
    void testIncrementCreditUsed_ExistingNetworkId() {
        creditUsed.put(1, new AtomicLong(50));
        creditHandler.incrementCreditUsed(1, 100);
        assertEquals(150, creditUsed.get(1).get());
    }

    @Test
    void testRemoveCreditUsed_ExistingNetworkId() {
        creditUsed.put(1, new AtomicLong(50));
        creditHandler.removeCreditUsed("1");
        assertFalse(creditUsed.containsKey(1));
    }

    @Test
    void testRemoveCreditUsed_NonExistingNetworkId() {
        creditHandler.removeCreditUsed("1");
        assertFalse(creditUsed.containsKey(1));
    }

    @Test
    void testGetCreditUsedByServiceProvider_PositiveCredits() {
        ConcurrentMap<Integer, AtomicInteger> counter = new ConcurrentHashMap<>();
        counter.put(1, new AtomicInteger(50));

        List<Map<String, Object>> result = creditHandler.getCreditUsedByServiceProvider(counter).collectList().block();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(50, result.getFirst().get("creditUsed"));
    }

    @Test
    void testGetCreditUsedByServiceProvider_NoCredits() {
        ConcurrentMap<Integer, AtomicInteger> counter = new ConcurrentHashMap<>();
        counter.put(1, new AtomicInteger(0));

        List<Map<String, Object>> result = creditHandler.getCreditUsedByServiceProvider(counter).collectList().block();

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @SneakyThrows
    @Test
    void testProcessAndSendData_BodyEmpty() {
        creditHandler.processAndSendData();

        verifyNoInteractions(httpClient);
    }

    @Test
    @SneakyThrows
    @SuppressWarnings("unchecked")
    void testProcessAndSendData_statusCode200() {
        creditUsed.put(1, new AtomicLong(100));
        setupMockProperties("http://localhost:8080/", "test-api-key");

        HttpResponse<String> httpResponse = mock(HttpResponse.class);
        when(httpResponse.statusCode()).thenReturn(HttpStatus.OK.value());

        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);

        when(httpClient.send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class))).thenReturn(httpResponse);

        creditHandler.processAndSendData();

        verify(httpClient).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

        assertEquals(0, creditUsed.get(1).get());

        HttpRequest capturedRequest = requestCaptor.getValue();
        assertEquals(URI.create("http://localhost:8080/instance"), capturedRequest.uri());
        assertRequestHeaders(capturedRequest);
    }

    @Test
    @SneakyThrows
    @SuppressWarnings("unchecked")
    void testProcessAndSendData_statusCode_500() {
        creditUsed.put(1, new AtomicLong(50));
        setupMockProperties("http://localhost/", "key");

        HttpResponse<String> httpResponse = mock(HttpResponse.class);
        when(httpResponse.statusCode()).thenReturn(HttpStatus.INTERNAL_SERVER_ERROR.value());

        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);

        when(httpClient.send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class))).thenReturn(httpResponse);

        creditHandler.processAndSendData();

        verify(httpClient).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @SneakyThrows
    @Test
    @SuppressWarnings("unchecked")
    void testProcessAndSendData_FailedRequest() {
        creditUsed.put(1, new AtomicLong(50));
        setupMockProperties("http://localhost/", "key");

        doThrow(new IOException("Connection error")).when(httpClient).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

        creditHandler.processAndSendData();

        assertEquals(50, creditUsed.get(1).get());
    }

    @SneakyThrows
    @Test
    @SuppressWarnings("unchecked")
    void testProcessAndSendData_CatchBlockExecution() {
        creditUsed.put(1, new AtomicLong(50));
        setupMockProperties("http://localhost/", "key");

        doThrow(new RuntimeException("Unexpected error"))
                .when(httpClient).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

        creditHandler.processAndSendData();

        verify(httpClient).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
        assertEquals(50, creditUsed.get(1).get());
    }
}
