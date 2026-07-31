package paicbd.smsc.routing.component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.paicbd.smsc.utils.Converter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;
import paicbd.smsc.routing.util.AppProperties;
import reactor.core.publisher.Flux;

@Slf4j
@Component
@RequiredArgsConstructor
public class CreditHandler {
    private static final String X_API_KEY = "X-API-Key";

    private final HttpClient httpClient;
    private final AppProperties appProperties;
    private final ConcurrentMap<Integer, AtomicLong> creditUsed;

    @Async
    @Scheduled(fixedRateString = "${backend.RequestFrequency}")
    public void processAndSendData() {
        ConcurrentMap<Integer, AtomicInteger> copy = new ConcurrentHashMap<>();
        this.creditUsed.forEach((k, v) -> copy.put(k, new AtomicInteger((int) v.get())));
        List<Map<String, Object>> body = this.getCreditUsedByServiceProvider(copy).collectList().block();
        Assert.notNull(body, "The body is null. No request will be sent to rating service");

        if (body.isEmpty()) {
            log.debug("The body is empty. No request will be sent to rating service");
            return;
        }

        try {
            log.debug("Sending request {} to rating service", body);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(this.appProperties.getBackendUrl().concat(this.appProperties.getInstanceName())))
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .header(X_API_KEY, this.appProperties.getBackendApiKey())
                    .POST(HttpRequest.BodyPublishers.ofString(Converter.valueAsString(body), StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (Objects.equals(response.statusCode(), HttpStatus.OK.value())) {
                log.debug("Request sent successfully to rating service with status code 200");
                copy.forEach((k, v) -> this.creditUsed.get(k).addAndGet(-v.get()));
                return;
            }
            log.warn("The request returned the following. Status code: {}. Response: {}. Trying again in the next iteration.", response.statusCode(), response.body());
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            log.debug("Error while sending request to rating service: {}", e.getMessage());
        }
    }

    public Flux<Map<String, Object>> getCreditUsedByServiceProvider(ConcurrentMap<Integer, AtomicInteger> counter) {
        log.debug("********* TODO: Pending to implement credits for MO messages *********");
        return Flux.fromIterable(counter.entrySet())
                .filter(entry -> entry.getValue().get() > 0)
                .mapNotNull(entry -> Map.of("networkId", entry.getKey(), "creditUsed", entry.getValue().get()));
    }

    public void incrementCreditUsed(Integer networkId, int totalUsed) {
        creditUsed.computeIfAbsent(networkId, k -> new AtomicLong(0)).addAndGet(totalUsed);
    }

    public void removeCreditUsed(String networkId) {
        creditUsed.remove(Integer.parseInt(networkId));
    }
}
