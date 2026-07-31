package paicbd.smsc.routing.util;

import com.paicbd.smsc.utils.Generated;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Getter
@Component
@Generated
public class AppProperties {
	@Value("${spring.application.name}")
    private String instanceName = "routing";
	
    @Value("#{'${redis.cluster.nodes}'.split(',')}")
    private List<String> redisNodes;

    @Value("${redis.threadPool.maxTotal}")
    private int redisMaxTotal;

    @Value("${redis.threadPool.maxIdle}")
    private int redisMaxIdle;

    @Value("${redis.threadPool.minIdle}")
    private int redisMinIdle;

    @Value("${redis.threadPool.blockWhenExhausted}")
    private boolean redisBlockWhenExhausted;

    @Value("${redis.connection.timeout:0}")
    private int redisConnectionTimeout;

    @Value("${redis.so.timeout:0}")
    private int redisSoTimeout;

    @Value("${redis.maxAttempts:0}")
    private int redisMaxAttempts;

    @Value("${redis.connection.password:}")
    private String redisPassword;

    @Value("${redis.connection.user:}")
    private String redisUser;

    @Value("${websocket.server.enabled}")
    private boolean wsEnabled;

    @Value("${websocket.server.host}")
    private String wsHost;

    @Value("${websocket.server.port}")
    private int wsPort;

    @Value("${websocket.server.path}")
    private String wsPath;

    @Value("${websocket.header.name}")
    private String wsHeaderName;

    @Value("${websocket.retry.intervalSeconds}")
    private int wsRetryInterval;

    @Value("${websocket.header.value}")
    private String wsHeaderValue;

    @Value("${backend.url}")
    private String backendUrl;
    
    @Value("${backend.apiKey}")
    private String backendApiKey;

    @Value("${scylla.contact.points}")
    private String scyllaContactPoints;

    @Value("${scylla.datacenter}")
    private String scyllaDatacenter;

    @Value("${scylla.user}")
    private String scyllaUser;

    @Value("${scylla.password}")
    private String scyllaPassword;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${spring.kafka.listener.concurrency}")
    private int kafkaListenerConcurrency;

    @Value("${kafka.listener.high.concurrency:3}")
    private int kafkaHighPriorityConcurrency;

    @Value("${kafka.listener.medium.concurrency:2}")
    private int kafkaMediumPriorityConcurrency;

    @Value("${kafka.listener.low.concurrency:1}")
    private int kafkaLowPriorityConcurrency;

    @Value("${tlv.sccp.called-addr.sri}")
    private short tlvSccpCalledAddrSri;

    @Value("${smsc.split.msg.reference.type:8BIT}")
    private String splitMsgReferenceType;

    @Value("${smsc.default.dlr.data-coding:0}")
    private int smscDefaultDlrDataCoding;

    @Value("${spring.kafka.consumer.reconnect.backoff.ms:1000}")
    private long kafkaConsumerReconnectBackoffMs;

    @Value("${spring.kafka.consumer.reconnect.backoff.max.ms:10000}")
    private long kafkaConsumerReconnectBackoffMaxMs;

    @Value("${spring.kafka.consumer.session.timeout.ms:30000}")
    private int kafkaConsumerSessionTimeoutMs;

    @Value("${spring.kafka.consumer.heartbeat.interval.ms:10000}")
    private int kafkaConsumerHeartbeatIntervalMs;

    @Value("${spring.kafka.producer.reconnect.backoff.ms:1000}")
    private long kafkaProducerReconnectBackoffMs;

    @Value("${spring.kafka.producer.reconnect.backoff.max.ms:10000}")
    private long kafkaProducerReconnectBackoffMaxMs;
}

