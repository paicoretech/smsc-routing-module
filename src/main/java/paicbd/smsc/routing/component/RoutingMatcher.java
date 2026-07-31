package paicbd.smsc.routing.component;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessageRegister;
import com.paicbd.smsc.dto.RoutingRule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.smsc.utils.GeneralSmscConstants.HTTP_PROTOCOL;
import static com.paicbd.smsc.utils.GeneralSmscConstants.SMPP_PROTOCOL;

@Slf4j
@Component
@RequiredArgsConstructor
public class RoutingMatcher {
    private final ConcurrentMap<Integer, List<RoutingRule>> routingRules;

    public RoutingRule getRouting(MessageEvent messageEvent) {
        if (Objects.isNull(this.routingRules.get(messageEvent.getOriginNetworkId()))) {
            return null;
        }
        Optional<RoutingRule> optionalRouting = this.routingRules.get(messageEvent.getOriginNetworkId())
                .stream()
                .filter(routing -> this.matchesRoutingCriteria(messageEvent, routing))
                .findFirst();
        return optionalRouting.orElse(null);
    }

    public RoutingRule getRoutingForAlternative(MessageEvent messageEvent) {
        List<RoutingRule> rules = this.routingRules.get(messageEvent.getOriginNetworkId());
        if (Objects.isNull(rules)) {
            return null;
        }
        Optional<RoutingRule> optionalRouting = rules
                .stream()
                .filter(routing -> routing.getId() == messageEvent.getRoutingId())
                .findFirst();
        return optionalRouting.orElse(null);
    }

    public RoutingRule.Destination getDestinationByRouting(RoutingRule routing) {
        if (Objects.isNull(routing)) {
            return null;
        }

        if (Objects.isNull(routing.getDestination())) {
            return null;
        }

        Optional<RoutingRule.Destination> optionalDestination = routing.getDestination().stream().min(Comparator.comparingInt(RoutingRule.Destination::getPriority));
        return optionalDestination.orElse(null);
    }



    public RoutingRule.Destination getDestinationForAlternative(RoutingRule routing, MessageEvent event) {
        if (Objects.isNull(routing)) {
            return null;
        }

        if (Objects.isNull(routing.getDestination())) {
            return null;
        }
        this.setRetryDestNetworkId(event, event.getDestNetworkId());
        List<RoutingRule.Destination> orderDestinations = routing.getDestination().stream()
                .sorted(Comparator.comparingInt(RoutingRule.Destination::getPriority)).toList();
        for (RoutingRule.Destination destination : orderDestinations) {
            if (!destNetworkContained(event.getRetryDestNetworkId(), destination.getNetworkId())) {
                return destination;
            }
        }
        return null;
    }

    private boolean matchesRoutingCriteria(MessageEvent messageEvent, RoutingRule routing) {
        log.debug("Checking Rule with id {}", routing.getId());
        if (!routing.isHasFilterRules()) {
            return true;
        }

        return this.matchesRegex(messageEvent.getSourceAddr(), routing.getOriginRegexSourceAddr(), "source address") &&
               this.matchesRegex(String.valueOf(messageEvent.getSourceAddrTon()), routing.getOriginRegexSourceAddrTon(), "source address TON") &&
               this.matchesRegex(String.valueOf(messageEvent.getSourceAddrNpi()), routing.getOriginRegexSourceAddrNpi(), "source address NPI") &&
               this.matchesRegex(messageEvent.getDestinationAddr(), routing.getOriginRegexDestinationAddr(), "destination address") &&
               this.matchesRegex(String.valueOf(messageEvent.getDestAddrNpi()), routing.getOriginRegexDestAddrNpi(), "destination address NPI") &&
               this.matchesRegex(String.valueOf(messageEvent.getDestAddrTon()), routing.getOriginRegexDestAddrTon(), "destination address TON") &&
               this.matchesRegex(String.valueOf(messageEvent.getImsi()), routing.getRegexImsiDigitsMask(), "IMSI Digits") &&
               this.matchesRegex(String.valueOf(messageEvent.getNetworkNodeNumber()), routing.getRegexNetworkNodeNumber(), "Network Node Number") &&
               this.matchesRegex(String.valueOf(messageEvent.getSccpCallingPartyAddress()), routing.getRegexCallingPartyAddress(), "SCCP Calling Party Address") &&
               this.matchesRegex(messageEvent.getShortMessage(), routing.getRegexShortMessage(), "Short Message") &&
               this.matchCustomParams(messageEvent.getCustomParams(), routing.getCustomParamsMatcher()) &&
               routing.isSriResponse() == messageEvent.isSriResponse();
    }

    private boolean matchesRegex(String value, String regex, String validation) {
        value = Optional.ofNullable(value).orElse("");
        regex = Optional.ofNullable(regex).orElse("");
        if (!regex.isEmpty() && !value.matches(regex.replace("\\\\", "\\"))) {
            log.debug("Regex {} does not match value {} trying to validate {}", regex, value, validation);
            return false;
        }
        log.debug("No regex found or it matches");
        return true;
    }

    private boolean matchCustomParams(Map<String, Object> customParam, List<RoutingRule.CustomParamMatcher> matcherList) {
        boolean hasMatcher = matcherList != null && !matcherList.isEmpty();
        boolean hasCustomParam = !Optional.ofNullable(customParam).orElse(Collections.emptyMap()).isEmpty();

        if (!hasMatcher) {
            return true;
        }

        if (!hasCustomParam) {
            return false;
        }

        for (RoutingRule.CustomParamMatcher matcher : matcherList) {
            String[] keys = matcher.getPropertyName().split("\\.");
            if (keys[0].equalsIgnoreCase("customParams")) {
                keys = Arrays.copyOfRange(keys, 1, keys.length);
            }

            Object value = getNestedValue(customParam, keys, 0);
            if (!Objects.equals(value, matcher.getValueMatcher())) {
                log.debug("The value '{}' does not match the matcher '{}'", value, matcher.getValueMatcher());
                return false;
            }
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    private Object getNestedValue(Map<String, Object> map, String[] keys, int index) {
        if (index >= keys.length) {
            return null;
        }

        Object value = map.get(keys[index]);
        if (index == keys.length - 1) {
            return value;
        }

        if (value instanceof Map) {
            return getNestedValue((Map<String, Object>) value, keys, index + 1);
        }
        return null;
    }

    public RoutingRule.Destination validateAndGetNextNonSipDestination(RoutingRule.Destination currentDestination, List<RoutingRule.Destination> destinations, MessageEvent event, MessageRegister registerData) {
        boolean sipMessageSupported = Objects.nonNull(registerData);
        event.setSipMessageSupported(sipMessageSupported);

        if (!sipMessageSupported) {
            log.warn("Destination address {} not supported SIP message, looking for another destination route", event.getDestinationAddr());
            List<RoutingRule.Destination> orderDestinations = destinations.stream()
                    .sorted(Comparator.comparingInt(RoutingRule.Destination::getPriority)).toList();
            for (RoutingRule.Destination destination : orderDestinations) {
                boolean validDestinationRoute = !"SIP".equalsIgnoreCase(destination.getProtocol()) && destination.getNetworkId() != event.getDestNetworkId()
                        && !destNetworkContained(event.getRetryDestNetworkId(), destination.getNetworkId());
                if (validDestinationRoute) {
                    return destination;
                } else {
                    setRetryDestNetworkId(event, destination.getNetworkId());
                }
            }

            return null;
        } else if (isGatewayOriginatedFromSmppOrHttp(event)) {
            event.setRegisteredDelivery(0);
        }

        event.setDestinationUri(registerData.getAor());
        return currentDestination;
    }

    private boolean isGatewayOriginatedFromSmppOrHttp(MessageEvent event) {
        return (SMPP_PROTOCOL.equals(event.getOriginProtocol()) || HTTP_PROTOCOL.equals(event.getOriginProtocol()))
                && "GW".equalsIgnoreCase(event.getOriginNetworkType());
    }

    private boolean destNetworkContained(String stringList, int currentNetworkId) {
        return Arrays.stream(stringList.split(",")).toList().contains(String.valueOf(currentNetworkId));
    }

    private void setRetryDestNetworkId(MessageEvent messageEvent, int destinationNetworkId) {
        String existingRetryDestNetworkId = messageEvent.getRetryDestNetworkId();
        if (Objects.isNull(existingRetryDestNetworkId) || existingRetryDestNetworkId.isEmpty()) {
            messageEvent.setRetryDestNetworkId(destinationNetworkId + "");
        } else {
            messageEvent.setRetryDestNetworkId(existingRetryDestNetworkId + "," + destinationNetworkId);
        }
    }
}
