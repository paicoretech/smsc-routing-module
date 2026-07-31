package paicbd.smsc.routing.processor;

import com.paicbd.smsc.dto.MessageEvent;

public interface RoutingProcessor {
    void prepareMessage(MessageEvent messageEvent);
}
