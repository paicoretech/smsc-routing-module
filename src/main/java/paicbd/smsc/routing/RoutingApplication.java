package paicbd.smsc.routing;

import com.paicbd.smsc.utils.Generated;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@Generated
@EnableAsync
@EnableScheduling
@SpringBootApplication
public class RoutingApplication {
	public static void main(String[] args) {
		SpringApplication.run(RoutingApplication.class, args);
	}
}
