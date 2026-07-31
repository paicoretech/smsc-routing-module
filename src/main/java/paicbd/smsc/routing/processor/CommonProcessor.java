package paicbd.smsc.routing.processor;

import com.paicbd.smsc.cdr.CdrProcessor;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessageRegister;
import com.paicbd.smsc.dto.RoutingRule;
import com.paicbd.smsc.dto.SipGateways;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.Ss7Settings;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.dto.diameter.DiameterConfig;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.kafka.KafkaUtils;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.ChargingUtils;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.DndType;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.utils.RegEventState;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.utils.SmppUtils;
import com.paicbd.smsc.utils.UtilsEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.bean.DeliveryReceipt;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.GSMSpecificFeature;
import org.jsmpp.bean.MessageMode;
import org.jsmpp.bean.MessageType;
import org.jsmpp.bean.OptionalParameter;
import org.jsmpp.util.DeliveryReceiptState;
import org.jsmpp.util.InvalidDeliveryReceiptException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import paicbd.smsc.routing.component.CreditHandler;
import paicbd.smsc.routing.component.RoutingMatcher;
import paicbd.smsc.routing.loaders.SettingsLoader;
import paicbd.smsc.routing.util.AppProperties;
import paicbd.smsc.routing.util.StaticMethods;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.paicbd.smsc.scylla.ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE;
import static com.paicbd.smsc.scylla.ScyllaTablesConstants.SIP_SUBMIT_SM_RESULT_TABLE;
import static com.paicbd.smsc.scylla.ScyllaTablesConstants.SMPP_SUBMIT_SM_RESULT_TABLE;
import static com.paicbd.smsc.utils.GeneralSmscConstants.AUTO_MAP_VERSION;
import static com.paicbd.smsc.utils.GeneralSmscConstants.HAS_ACTION_ADVANCED_RULES;
import static com.paicbd.smsc.utils.GeneralSmscConstants.MAP_VERSION;
import static com.paicbd.smsc.utils.GeneralSmscConstants.MEDIUM_PRIORITY;
import static com.paicbd.smsc.utils.GeneralSmscConstants.MSG_REFERENCE_TYPE;
import static com.paicbd.smsc.utils.GeneralSmscConstants.MSG_REFERENCE_TYPE_8BIT;
import static com.paicbd.smsc.utils.GeneralSmscConstants.SCCP_DESTINATION_ADDRESS_SRI;
import static com.paicbd.smsc.utils.GeneralSmscConstants.SIP_PROTOCOL;
import static com.paicbd.smsc.utils.GeneralSmscConstants.SMSC_GENERATED_DLR;
import static com.paicbd.smsc.utils.GeneralSmscConstants.USE_DND_FILTERING;

@Slf4j
@Component
@RequiredArgsConstructor
public class CommonProcessor {
    private static final Set<String> DELIVERY_STATUS = Set.of("ENROUTE", "DELIVRD", "ACCEPTD");
    private static final int SUCCESS_STATUS = 0;
    private static final int FAILURE_STATUS = 1;
    private static final int DEFAULT_MODE_ESM_CLASS = 3;
    private static final int DEFAULT_MODE_WITH_UDH_ESM_CLASS = 64;

    private final CdrProcessor cdrProcessor = new CdrProcessor();

    private final AtomicInteger idPartMessages = new AtomicInteger(0);
    private final CreditHandler creditHandler;
    private final SettingsLoader settingsLoader;
    private final RoutingMatcher routingMatcher;
    private final ConcurrentMap<Integer, Gateway> gateways;
    private final ConcurrentMap<Integer, ServiceProvider> serviceProviders;
    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ConcurrentMap<Integer, DiameterConfig> diameterGatewayMap;
    private final AppProperties appProperties;
    private final ConcurrentMap<Integer, SipGateways> sipGatewaysMap;

    public void setUpInitialSettings(MessageEvent event) {
        GeneralSettings smppHttpSettings = this.settingsLoader.getSmppHttpSettings();
        event.setSourceAddrTon(Objects.isNull(event.getSourceAddrTon()) ? smppHttpSettings.getSourceAddrTon() : event.getSourceAddrTon());
        event.setSourceAddrNpi(Objects.isNull(event.getSourceAddrNpi()) ? smppHttpSettings.getSourceAddrNpi() : event.getSourceAddrNpi());
        event.setDestAddrTon(Objects.isNull(event.getDestAddrTon()) ? smppHttpSettings.getDestAddrTon() : event.getDestAddrTon());
        event.setDestAddrNpi(Objects.isNull(event.getDestAddrNpi()) ? smppHttpSettings.getDestAddrNpi() : event.getDestAddrNpi());
        event.setDataCoding(Objects.isNull(event.getDataCoding()) ? smppHttpSettings.getEncodingGsm7() : event.getDataCoding());
        event.setEsmClass(Objects.isNull(event.getEsmClass()) ? 0 : event.getEsmClass());

        adjustValidityPeriod(event, smppHttpSettings);
    }

    private void adjustValidityPeriod(MessageEvent event, GeneralSettings smppHttpSettings) {
        long smscValidityPeriod;
        if (Objects.isNull(event.getStringValidityPeriod())) {
            long messageValidityPeriod = event.getValidityPeriod() == 0
                    ? smppHttpSettings.getValidityPeriod()
                    : event.getValidityPeriod();
            smscValidityPeriod = getAdjustedValidityPeriod(messageValidityPeriod, smppHttpSettings.getMaxValidityPeriod());
            event.setStringValidityPeriod(Converter.secondsToRelativeValidityPeriod(smscValidityPeriod));
        } else {
            long seconds = Converter.smppValidityPeriodToSeconds(event.getStringValidityPeriod());
            smscValidityPeriod = getAdjustedValidityPeriod(seconds, smppHttpSettings.getMaxValidityPeriod());
        }
        event.setValidityPeriod(smscValidityPeriod);
    }

    private long getAdjustedValidityPeriod(long validityPeriod, long maxValidityPeriod) {
        return Math.min(validityPeriod, maxValidityPeriod);
    }

    private void verifyAndCompleteDLR(MessageEvent messageEvent, RoutingRule routing) {
        if (messageEvent.isDlr()) {
            messageEvent.setDelReceipt(messageEvent.getShortMessage());
            messageEvent.setSystemId(null);
            messageEvent.setRegisteredDelivery(RequestDelivery.NON_REQUEST_DLR.getValue());
            return;
        }

        messageEvent.setDiameterCharging(routing.isDiameterCharging());
    }

    private void applyRegisterDelivery(MessageEvent messageEvent, RoutingRule.Destination destination) {
        if ("GW".equalsIgnoreCase(destination.getNetworkType()) && !(GeneralSmscConstants.SS7_PROTOCOL.equalsIgnoreCase(destination.getProtocol()) ||
                GeneralSmscConstants.DIAMETER_PROTOCOL.equalsIgnoreCase(destination.getProtocol()) ||
                GeneralSmscConstants.SIP_PROTOCOL.equalsIgnoreCase(destination.getProtocol()))) {
            var gateway = this.gateways.get(destination.getNetworkId());
            var requestDlr = RequestDelivery.fromInt(gateway.getRequestDLR());
            if (requestDlr != RequestDelivery.TRANSPARENT) {
                messageEvent.setRegisteredDelivery(requestDlr == RequestDelivery.REQUEST_DLR ? 1 : 0);
            }
        }
    }

    private void processBroadcastMessageIfApply(MessageEvent messageEvent) {
        int esmClass = Optional.ofNullable(messageEvent.getEsmClass()).orElse(DEFAULT_MODE_ESM_CLASS);
        boolean isMessageFromBroadcast = Objects.nonNull(messageEvent.getBroadcastId());
        boolean applyForUdh = EncodingUtils.udhEsmeClassValues.contains(esmClass);
        if (isMessageFromBroadcast) {
            if (applyForUdh) {
                log.error("UDH is not supported for broadcast messages");
                messageEvent.setErrorCode(ErrorCodes.NOT_SUPPORTED);
                this.sendFailedCdr(messageEvent);
                return;
            }
            this.prepareMessageBytes(messageEvent);
            messageEvent.setUdhLength(0);
            messageEvent.setUdhRaw(Set.of());
            messageEvent.setUdhBytes(new byte[]{});
        }
    }

    private void prepareMessageBytes(MessageEvent messageEvent) {
        byte[] bytes;
        boolean isHexadecimal = EncodingUtils.isHexadecimal(messageEvent.getShortMessage().trim());
        int encodingType = SmppUtils.determineEncodingType(messageEvent.getDataCoding(), settingsLoader.getSmppHttpSettings());
        if (EncodingUtils.GSM7_DATA_CODINGS.contains(messageEvent.getDataCoding())) {
            if (isHexadecimal) {
                String sm = EncodingUtils.decodeMessage(messageEvent.getMessageBytes(), encodingType);
                bytes = EncodingUtils.encodeMessage(sm, encodingType);
            } else {
                bytes = EncodingUtils.encodeMessage(messageEvent.getShortMessage(), EncodingUtils.GSM7);
            }
        } else if (EncodingUtils.UCS2_DATA_CODINGS.contains(messageEvent.getDataCoding())) {
            if (isHexadecimal) {
                String sm = EncodingUtils.decodeMessage(messageEvent.getMessageBytes(), encodingType);
                bytes = EncodingUtils.encodeMessage(sm, encodingType);
            } else {
                bytes = EncodingUtils.encodeMessage(messageEvent.getShortMessage(), EncodingUtils.UCS2);
            }
        } else {
            bytes = messageEvent.getShortMessage().getBytes(StandardCharsets.ISO_8859_1);
            messageEvent.setShortMessage(EncodingUtils.bytesToHex(bytes));
        }
        messageEvent.setMessageBytes(bytes);
    }

    private void applyRoutingRulesActions(MessageEvent event, RoutingRule routing) {
        if (routing.isHasActionRules()) {
            this.replaceSourceAddrIfNeeded(event, routing);
            this.replaceDestinationAddrIfNeeded(event, routing);
            this.replaceShortMessageIfNeeded(event, routing);
        }
        if (!routing.isAutoMapVersion()) {
            event.setMapVersion(routing.getActionAdvanced().getMapVersion());
        }
        this.setSS7RulesConfiguration(event, routing);
    }

    private void replaceSourceAddrIfNeeded(MessageEvent event, RoutingRule routing) {
        if (!routing.getNewSourceAddr().isEmpty()) {
            log.debug("Replace source address form {} to {}", event.getSourceAddr(), routing.getNewSourceAddr());
            event.setSourceAddr(routing.getNewSourceAddr());
        }
        if (routing.getNewSourceAddrTon() > -1) {
            log.debug("Replace source address TON form {} to {}", event.getSourceAddrTon(), routing.getNewSourceAddrTon());
            event.setSourceAddrTon(routing.getNewSourceAddrTon());
        }
        if (routing.getNewSourceAddrNpi() > -1) {
            log.debug("Replace source address NPI form {} to {}", event.getSourceAddrNpi(), routing.getNewSourceAddrNpi());
            event.setSourceAddrNpi(routing.getNewSourceAddrNpi());
        }
        if (!routing.getRemoveSourceAddrPrefix().isEmpty()) {
            log.debug("Removing source address prefix if needed, {}", routing.getRemoveSourceAddrPrefix());
            event.setSourceAddr(event.getSourceAddr().replaceFirst(routing.getRemoveSourceAddrPrefix(), ""));
        }
        if (!routing.getAddSourceAddrPrefix().isEmpty()) {
            log.debug("Adding source address prefix, {}", routing.getAddSourceAddrPrefix());
            event.setSourceAddr(routing.getAddSourceAddrPrefix().concat(event.getSourceAddr()));
        }
    }

    private void replaceDestinationAddrIfNeeded(MessageEvent event, RoutingRule routing) {
        if (!routing.getNewDestinationAddr().isEmpty()) {
            log.debug("Replace destination address form {} to {}", event.getDestinationAddr(), routing.getNewDestinationAddr());
            event.setDestinationAddr(routing.getNewDestinationAddr());
        }
        if (routing.getNewDestAddrTon() > -1) {
            log.debug("Replace destination address TON form {} to {}", event.getDestAddrTon(), routing.getNewDestAddrTon());
            event.setDestAddrTon(routing.getNewDestAddrTon());
        }
        if (routing.getNewDestAddrNpi() > -1) {
            log.debug("Replace destination address NPI form {} to {}", event.getDestAddrNpi(), routing.getNewDestAddrNpi());
            event.setDestAddrNpi(routing.getNewDestAddrNpi());
        }
        if (!routing.getRemoveDestAddrPrefix().isEmpty()) {
            log.debug("Removing destination prefix if needed, {}", routing.getRemoveDestAddrPrefix());
            event.setDestinationAddr(event.getDestinationAddr().replaceFirst(routing.getRemoveDestAddrPrefix(), ""));
        }
        if (!routing.getAddDestAddrPrefix().isEmpty()) {
            log.debug("Adding destination prefix, {}", routing.getAddDestAddrPrefix());
            event.setDestinationAddr(routing.getAddDestAddrPrefix().concat(event.getDestinationAddr()));
        }
    }

    private void replaceShortMessageIfNeeded(MessageEvent event, RoutingRule routing) {
        if (!Optional.ofNullable(routing.getNewShortMessage()).orElse("").isEmpty()) {
            log.debug("Input short message: {}", event.getShortMessage());
            List<String> regexList = StaticMethods.extractRegexFromMessage(routing.getNewShortMessage());
            List<String> matches = StaticMethods.applyRegexToInput(event.getShortMessage(), regexList);
            String newMessage = StaticMethods.buildOutput(routing.getNewShortMessage(), matches);
            log.debug("Short message before regex: {}", event.getShortMessage());
            event.setShortMessage(newMessage);
            event.setDelReceipt(newMessage);
        }

        log.debug("Short message after regex: {}", event.getShortMessage());
    }

    private void setSS7RulesConfiguration(MessageEvent event, RoutingRule rule) {
        this.setSs7PropertiesFromMessageEvent(event);
        event.setDropMapSri(rule.isDropMapSri());
        event.setNetworkIdToMapSri(rule.getNetworkIdToMapSri());
        event.setNetworkIdToPermanentFailure(rule.getNetworkIdToPermanentFailure());
        event.setDropTempFailure(rule.isDropTempFailure());
        event.setNetworkIdTempFailure(rule.getNetworkIdTempFailure());
        event.setSriResponse(rule.isSriResponse());
        event.setCheckSriResponse(rule.isCheckSriResponse());
        if (!rule.getNewGtSccpAddr().isEmpty()) {
            Optional<String> newGtSccpAddrOptional = StaticMethods.processReplaceAction(rule.getNewGtSccpAddr(), event);
            if (newGtSccpAddrOptional.isEmpty()) {
                log.warn("No replacement found for GT SCCP address in rule {} with template {}", rule.getId(), rule.getNewGtSccpAddr());
                return;
            }
            log.debug("Replace SMSC Global title  form {} to {}", event.getGlobalTitle(), newGtSccpAddrOptional.get());
            event.setGlobalTitle(newGtSccpAddrOptional.get());
        }
    }

    private void setSs7PropertiesFromSettings(MessageEvent event) {
        Ss7Settings ss7Config = this.settingsLoader.getSs7Settings(event.getDestNetworkId());
        if (Objects.isNull(ss7Config)) {
            log.error("No SS7 settings found for network id {}", event.getDestNetworkId());
            return;
        }
        event.setGlobalTitle(ss7Config.getGlobalTitle());
        event.setGlobalTitleIndicator(ss7Config.getGlobalTitleIndicator().toString());
        event.setTranslationType(ss7Config.getTranslationType());
        event.setSmscSsn(ss7Config.getSmscSsn());
        event.setHlrSsn(ss7Config.getHlrSsn());
        event.setMscSsn(ss7Config.getMscSsn());
        event.setMapVersion(ss7Config.getMapVersion());
    }

    private void setDiameterPropertiesFromSettings(MessageEvent event) {
        DiameterConfig diameterConfig = this.diameterGatewayMap.get(event.getDestNetworkId());
        if (Objects.isNull(diameterConfig)) {
            log.error("No Diameter settings found for network id {}", event.getDestNetworkId());
            return;
        }
        event.setGlobalTitle(diameterConfig.getGlobalTitle());
    }

    private void setSs7PropertiesFromMessageEvent(MessageEvent event) {
        event.setMsisdn(event.getDestinationAddr());
        event.setAddressNatureMsisdn(event.getDestAddrTon());
        event.setNumberingPlanMsisdn(event.getDestAddrNpi());
    }

    public MessagePart getPartsOfMessage(MessageEvent messageEvent) {
        final String shortMessage = messageEvent.getShortMessage().trim();
        final boolean isHex = EncodingUtils.isHexadecimal(shortMessage);
        final String protocol = messageEvent.getDestProtocol();
        boolean shouldSplit;
        boolean splitByUdh = true;

        switch (protocol) {
            case GeneralSmscConstants.SS7_PROTOCOL -> {
                Ss7Settings settings = this.settingsLoader.getSs7Settings(messageEvent.getDestNetworkId());
                Assert.notNull(settings, "No SS7 gateway found for network id " + messageEvent.getDestNetworkId());
                shouldSplit = settings.isSplitMessage();
            }
            case GeneralSmscConstants.DIAMETER_PROTOCOL -> {
                DiameterConfig diameterGateway = this.diameterGatewayMap.get(messageEvent.getDestNetworkId());
                Assert.notNull(diameterGateway, "No Diameter gateway found for network id " + messageEvent.getDestNetworkId());
                shouldSplit = diameterGateway.isSplitMessage();
            }
            case GeneralSmscConstants.SMPP_PROTOCOL -> {
                Gateway gateway = gateways.get(messageEvent.getDestNetworkId());
                Assert.notNull(gateway, "No SMPP gateway found for network id " + messageEvent.getDestNetworkId());
                shouldSplit = gateway.isSplitMessage();
                splitByUdh = "UDH".equalsIgnoreCase(gateway.getSplitSmppType());
            }
            case GeneralSmscConstants.SIP_PROTOCOL -> {
                SipGateways sipGateways = this.sipGatewaysMap.get(messageEvent.getDestNetworkId());
                Assert.notNull(sipGateways, "No SIP gateways found for network id " + messageEvent.getDestNetworkId());
                shouldSplit = sipGateways.isSplitMessage();
            }
            default -> throw new IllegalStateException("Unexpected value: " + protocol);
        }
        return this.getMessagePartsByMapSpecification(messageEvent, shortMessage, isHex, shouldSplit, splitByUdh);
    }


    private MessagePart getMessagePartsByMapSpecification(
            MessageEvent event, String shortMessage, boolean isHex, boolean shouldSplit, boolean splitByUdh) {
        int messageLength = isHex ? (shortMessage.length() / 2) : shortMessage.length();
        boolean use16Bit = !MSG_REFERENCE_TYPE_8BIT.equalsIgnoreCase(appProperties.getSplitMsgReferenceType());
        int concatUdhLength = use16Bit
                ? EncodingUtils.CONCATENATION_UDH_16BIT_LENGTH
                : EncodingUtils.CONCATENATION_UDH_8BIT_LENGTH;
        int maxLengthPerPart = StaticMethods.obtainSs7SmsLengthByDataCoding(event, shouldSplit, concatUdhLength);
        int parts = calculateParts(messageLength, maxLengthPerPart);

        return new MessagePart(
                shouldSplit ? parts : 1,
                shouldSplit ? maxLengthPerPart : messageLength,
                event.getDataCoding(),
                splitByUdh
        );
    }

    public static int calculateParts(int textLength, int maxLengthPerPart) {
        if (textLength <= 0) {
            return 1;
        }

        return (int) Math.ceil((double) textLength / maxLengthPerPart);
    }

    @Async
    public void processDlrInAsync(MessageEvent messageEvent) {
        this.processDlr(messageEvent);
    }

    private void setMessagePriorityForSmppOrHttpDlr(UtilsRecords.SubmitSmResponseEvent result, MessageEvent messageEvent) {
        if (Objects.isNull(result.customParams())) {
            log.warn("Custom Param is null assigning Medium Priority");
            messageEvent.setSmscMessagePriority(MEDIUM_PRIORITY);
            return;
        }
        String messagePriority = (String) result.customParams().get(GeneralSmscConstants.MESSAGE_PRIORITY);
        messageEvent.setSmscMessagePriority(Objects.isNull(messagePriority) ? MEDIUM_PRIORITY : messagePriority);
    }

    private void publishDlrInKafkaTopic(MessageEvent dlr, String dlrDestinationProtocol, String dlrDestinationNetworkType) {
        switch (dlrDestinationProtocol) {
            case "HTTP" -> this.kafkaTemplate.send(KafkaTopicsConstants.HTTP_DLR_TOPIC, dlr.toString());
            case "SMPP" -> {
                if ("SP".equalsIgnoreCase(dlrDestinationNetworkType)) {
                    this.kafkaTemplate.send(KafkaTopicsConstants.SMPP_DLR_TOPIC, dlr.toString());
                } else {
                    dlr.setDlr(false);
                    String topic = KafkaUtils.getDeliveryDestinationTopic(dlr);
                    this.kafkaTemplate.send(topic, dlr.toString());
                }
            }
            case "SS7" -> {
                Ss7Settings ss7Config = this.settingsLoader.getSs7Settings(dlr.getDestNetworkId());
                if (Objects.isNull(ss7Config)) {
                    log.error("No SS7 settings found for send DLR to network id {}", dlr.getDestNetworkId());
                    return;
                }
                this.setSs7PropertiesFromSettings(dlr);
                String topic = KafkaUtils.getDeliveryDestinationTopic(dlr);
                this.kafkaTemplate.send(topic, dlr.toString());
            }
            case GeneralSmscConstants.SIP_PROTOCOL -> {
                String topic = KafkaUtils.getDeliveryDestinationTopic(dlr);
                this.kafkaTemplate.send(topic, dlr.toString());
            }
            default -> {
                DiameterConfig diameterConfig = this.diameterGatewayMap.get(dlr.getDestNetworkId());
                if (Objects.isNull(diameterConfig)) {
                    log.error("No Diameter settings found for send DLR to network id {}", dlr.getDestNetworkId());
                    return;
                }
                dlr.setDlr(true);
                dlr.setGlobalTitle(diameterConfig.getGlobalTitle());
                String topic = KafkaUtils.getDeliveryDestinationTopic(dlr);
                this.kafkaTemplate.send(topic, dlr.toString());
            }
        }
    }

    private void handleReceiptDelivery(MessageEvent messageEvent, UtilsRecords.SubmitSmResponseEvent result) {
        if (Objects.nonNull(messageEvent.getDelReceipt())) {
            int encodingType = SmppUtils.determineEncodingType(messageEvent.getDataCoding(), settingsLoader.getSmppHttpSettings());
            try {
                DeliveryReceipt deliveryReceipt;
                if (EncodingUtils.isHexadecimal(messageEvent.getDelReceipt())) {
                    byte[] encodedDeliveryReceipt = EncodingUtils.hexToBytes(messageEvent.getDelReceipt());
                    String deliveryReceiptString = EncodingUtils.decodeMessage(encodedDeliveryReceipt, encodingType);
                    deliveryReceipt = new DeliveryReceipt(deliveryReceiptString);
                } else {
                    deliveryReceipt = new DeliveryReceipt(messageEvent.getDelReceipt());
                }
                deliveryReceipt.setId(result.submitSmServerId());
                messageEvent.setDelReceipt(deliveryReceipt.toString());
                messageEvent.setShortMessage(deliveryReceipt.toString());
                messageEvent.setMessageBytes(EncodingUtils.encodeMessage(deliveryReceipt.toString(), encodingType));
            } catch (InvalidDeliveryReceiptException e) {
                log.error("Error casting deliveryReceipt for {}", messageEvent.getDelReceipt());
            }
        }
    }

    private void encodeSmscGeneratedDlr(MessageEvent messageEvent) {
        boolean isSmscDlr = Boolean.parseBoolean(
                messageEvent.getFromCustomParam(SMSC_GENERATED_DLR, false).toString());
        if (!isSmscDlr) {
            return;
        }

        messageEvent.setDataCoding(appProperties.getSmscDefaultDlrDataCoding());
        String dlrMessage = messageEvent.getShortMessage();
        byte[] processedBytes;
        if (EncodingUtils.GSM7_DATA_CODINGS.contains(messageEvent.getDataCoding())) {
            processedBytes = EncodingUtils.encodeMessage(dlrMessage, EncodingUtils.GSM7);
        } else if (EncodingUtils.UCS2_DATA_CODINGS.contains(messageEvent.getDataCoding())) {
            processedBytes = EncodingUtils.encodeMessage(dlrMessage, EncodingUtils.UCS2);
        } else {
            processedBytes = dlrMessage.getBytes(StandardCharsets.ISO_8859_1);
        }
        messageEvent.setMessageBytes(processedBytes);
    }

    private void configureDlrMessageEventByMessageOriginProtocol(MessageEvent messageEvent, UtilsRecords.SubmitSmResponseEvent result) {
        if (GeneralSmscConstants.SS7_PROTOCOL.equalsIgnoreCase(result.originProtocol())) {
            this.configureMessageForSS7Protocol(messageEvent, result);
            return;
        }
        this.configureMessageForOtherProtocol(messageEvent, result);
    }

    private void configureMessageForSS7Protocol(MessageEvent messageEvent, UtilsRecords.SubmitSmResponseEvent result) {
        messageEvent.setDestNetworkType("GW");
        messageEvent.setDlr(true);
        messageEvent.setDeliverSmId(result.submitSmServerId());
        messageEvent.addCustomParam(result.customParams());
        setSs7PropertiesFromSettings(messageEvent);
        setSs7PropertiesFromMessageEvent(messageEvent);
        setCommandIdForDeliveryStatus(messageEvent);
        boolean allowAutoMap = Boolean.parseBoolean(messageEvent.getFromCustomParam(AUTO_MAP_VERSION, true).toString());
        if (!allowAutoMap) {
            int mapVersion = Integer.parseInt(messageEvent.getFromCustomParam(MAP_VERSION, messageEvent.getMapVersion()).toString());
            messageEvent.setMapVersion(mapVersion);
        }
    }

    private void setCommandIdForDeliveryStatus(MessageEvent event) {
        boolean isDelivered = DELIVERY_STATUS.contains(event.getStatus().toUpperCase());
        if (isDelivered) {
            event.setCommandStatus(SUCCESS_STATUS);
            return;
        }
        event.setCommandStatus(FAILURE_STATUS);
    }

    private void configureMessageForOtherProtocol(MessageEvent messageEvent, UtilsRecords.SubmitSmResponseEvent result) {
        messageEvent.setDestNetworkType("SP");
        messageEvent.setDeliverSmServerId(result.submitSmServerId());
        messageEvent.setSystemId(result.systemId());
        messageEvent.setDlr(true);
    }

    public void processCandidatesForLongMessages(MessageEvent messageEvent) {
        Integer dataCoding = Optional.ofNullable(messageEvent.getDataCoding()).orElse(0);
        int encoding = SmppUtils.determineEncodingType(dataCoding, settingsLoader.getSmppHttpSettings());
        MessagePart partMessage = getPartsOfMessage(messageEvent);
        int parts = partMessage.parts();

        boolean isHex = EncodingUtils.isHexadecimal(messageEvent.getShortMessage().trim());
        String currentMessage = isHex ? messageEvent.getShortMessage().trim() : messageEvent.getShortMessage();

        int length = partMessage.messageLength();
        int realLength = isHex ? length * 2 : length;

        if (parts <= 1) {
            int minLength = Math.min(messageEvent.getShortMessage().length(), realLength);
            String oneMessagePart = currentMessage.substring(0, minLength);
            messageEvent.setShortMessage(oneMessagePart);
            byte[] partBytes = isHex ? EncodingUtils.hexToBytes(oneMessagePart) : messageEvent.getMessageBytes();
            messageEvent.setMessageBytes(partBytes);
            return;
        }
        messageEvent.setSplitForSmsc(true);
        messageEvent.addCustomParam(MSG_REFERENCE_TYPE, appProperties.getSplitMsgReferenceType());
        removeMessagePayloadTlvIfPresent(messageEvent); // MessagePayload TLV is not supported for long messages
        String stringReference = String.valueOf(idPartMessages.incrementAndGet());
        List<com.paicbd.smsc.dto.MessagePart> messagePartList = new ArrayList<>();
        for (int i = 0; i < parts; i++) {
            com.paicbd.smsc.dto.MessagePart messagePartEvent = new com.paicbd.smsc.dto.MessagePart();
            messagePartEvent.setUdhRaw(new HashSet<>());
            ESMClass esm = new ESMClass(MessageMode.DEFAULT, MessageType.DEFAULT,
                    partMessage.isUdh() ? GSMSpecificFeature.UDHI : GSMSpecificFeature.DEFAULT);

            int startMessagePart = i * realLength;
            int endMessagePart = Math.min((i + 1) * realLength, currentMessage.length());

            if (isHex && (endMessagePart - startMessagePart) % 2 != 0) {
                endMessagePart = Math.max(startMessagePart, endMessagePart - 1);
            }

            String messagePart = currentMessage.substring(startMessagePart, endMessagePart);
            byte[] partBytes = isHex ? EncodingUtils.hexToBytes(messagePart) :
                    EncodingUtils.encodeMessage(messagePart, encoding);

            log.debug("Part {} of {}, message: {}", i + 1, parts, messagePart);
            messagePartEvent.setMessageId(messageEvent.getMessageId());
            messageEvent.setEsmClass((int) esm.value());
            messagePartEvent.setShortMessage(messagePart);
            messagePartEvent.setSegmentSequence(i + 1);
            messagePartEvent.setTotalSegment(parts);
            messagePartEvent.setMsgReferenceNumber(stringReference);
            messagePartEvent.setUdhBytes(messageEvent.getUdhBytes());
            messagePartEvent.setUdhRaw(messageEvent.getUdhRaw());
            messagePartEvent.setPartBytes(partBytes);
            messagePartEvent.setOptionalParameters(messageEvent.getOptionalParameters());
            messagePartList.add(messagePartEvent);
        }
        messageEvent.setMessageParts(messagePartList);
        log.debug("Message {} has {} parts and length {}", messageEvent.getMessageId(), parts, length);
    }

    public void processMessagePayloadTlvIfApply(MessageEvent messageEvent) {
        // If is SMPP and has MESSAGE_PAYLOAD TLV, then remove a short message and udh bytes, because we will send it as TLV
        boolean existsMessagePayloadTLV = existsMessagePayloadTLV(messageEvent);
        boolean isDestinationSMPP = GeneralSmscConstants.SMPP_PROTOCOL.equalsIgnoreCase(messageEvent.getDestProtocol());
        if (existsMessagePayloadTLV && isDestinationSMPP) {
            Gateway destinationGateway = gateways.get(messageEvent.getDestNetworkId());
            if (!destinationGateway.isSplitMessage()) {
                messageEvent.setShortMessage("");
                messageEvent.setMessageBytes(new byte[]{});
                messageEvent.setDelReceipt("");
                messageEvent.setUdhBytes(new byte[]{});
                messageEvent.setUdhRaw(new HashSet<>());
            }
        }
    }

    private void removeMessagePayloadTlvIfPresent(MessageEvent messageEvent) {
        if (messageEvent.getOptionalParameters() == null || messageEvent.getOptionalParameters().isEmpty()) {
            return;
        }
        messageEvent.getOptionalParameters().removeIf(
                optionalParameter ->
                        optionalParameter.tag() == OptionalParameter.Tag.MESSAGE_PAYLOAD.code()
        );
    }

    private boolean existsMessagePayloadTLV(MessageEvent messageEvent) {
        return messageEvent.getOptionalParameters() != null &&
                messageEvent.getOptionalParameters().stream()
                        .anyMatch(tlv -> tlv.tag() == OptionalParameter.Tag.MESSAGE_PAYLOAD.code());
    }

    private boolean isBlockedByDnd(MessageEvent messageEvent) {
        return checkAndBlockDnd(DndType.GLOBAL, messageEvent.getDestinationAddr(), DndType.GLOBAL.name(), "global", messageEvent)
                || checkAndBlockDnd(DndType.NETWORK_ID, messageEvent.getDestinationAddr(), String.valueOf(messageEvent.getOriginNetworkId()), "network", messageEvent)
                || checkAndBlockDnd(DndType.SENDER, messageEvent.getDestinationAddr(), messageEvent.getSourceAddr(), "sender", messageEvent);
    }

    private boolean checkAndBlockDnd(DndType type, String msisdn, String key, String logContext, MessageEvent messageEvent) {
        if (scyllaManager.isInDndWithExactKey(type.name(), msisdn, key)) {
            log.info("Number {} is in {} DND list ({}) - skipping.", msisdn, type, logContext);
            int errorCode = switch (type) {
                case GLOBAL -> ErrorCodes.BLOCKED_BY_DND_GLOBAL;
                case NETWORK_ID -> ErrorCodes.BLOCKED_BY_DND_NETWORK;
                case SENDER -> ErrorCodes.BLOCKED_BY_DND_SENDER;
            };
            messageEvent.setErrorCode(errorCode);
            return true;
        }
        return false;
    }

    @Generated
    public record MessagePart(
            int parts,
            int messageLength,
            int encoding,
            boolean isUdh
    ) {
    }

    public void publishInKafkaTopic(MessageEvent messageEvent) {
        String payload = messageEvent.toString();
        String kafkaTopic = "";
        try {
            if (!messageEvent.isDlr()) {
                kafkaTopic = KafkaUtils.getMessageDestinationTopic(messageEvent);
            } else if (messageEvent.isProcess()) {
                kafkaTopic = KafkaUtils.getDeliveryDestinationTopic(messageEvent);
            }
            kafkaTemplate.send(kafkaTopic, payload);
        } catch (Exception ex) {
            log.error("Error on try to publish on kafka", ex);
        }

    }

    private void sendFailedCdr(MessageEvent event) {
        cdrProcessor.publishCdr(event, UtilsEnum.Module.ROUTING, StaticMethods.getMessageType(event),
                UtilsEnum.CdrStatus.FAILED, kafkaTemplate);
    }

    // TODO Pending to Implement
    public Runnable incrementCreditUsedByOriginNetworkId(List<MessageEvent> messageList) {
        return () -> {
            Map<Integer, Integer> creditUsedByOriginNetworkId = messageList.stream()
                    .filter(event -> "SP".equalsIgnoreCase(event.getOriginNetworkType()))
                    .collect(Collectors.groupingBy(MessageEvent::getOriginNetworkId,
                            Collectors.summingInt(event -> event.getMessageParts() == null ? 1 : event.getMessageParts().size())));

            creditUsedByOriginNetworkId.forEach((k, v) ->
                    log.debug("Incrementing credit used for origin network id: {} by: {}", k, v));
            creditUsedByOriginNetworkId.forEach(creditHandler::incrementCreditUsed);
        };
    }

    private void setPrefixSccpCalledAddrSRIFromTLV(MessageEvent messageEvent) {

        if (appProperties.getTlvSccpCalledAddrSri() == -1 || messageEvent.getOptionalParameters() == null) {
            return;
        }

        for (UtilsRecords.OptionalParameter tlv : messageEvent.getOptionalParameters()) {
            if (tlv.tag() == appProperties.getTlvSccpCalledAddrSri()) {
                String value = tlv.value().trim().replace("\u0000", ""); // replace null value
                if (!value.isBlank() && value.matches("\\d+")) {
                    String destinationAddress = value + messageEvent.getDestinationAddr();
                    messageEvent.addCustomParam(SCCP_DESTINATION_ADDRESS_SRI, destinationAddress);
                }
                break;
            }
        }
    }

    /**
     * Sets both origin and destination network names on the MessageEvent for CDR
     * generation.
     * This method looks up the network names from the appropriate maps based on
     * a network type.
     *
     * @param messageEvent The message event to populate with network names
     */
    private void setNetworkNames(MessageEvent messageEvent) {
        // Set the origin network name
        String originNetworkName = getNetworkName(
                messageEvent.getOriginNetworkId(),
                messageEvent.getOriginNetworkType(),
                messageEvent.getOriginProtocol());
        messageEvent.setOriginNetworkName(originNetworkName);

        // Set the destination network name
        String destNetworkName = getNetworkName(
                messageEvent.getDestNetworkId(),
                messageEvent.getDestNetworkType(),
                messageEvent.getDestProtocol());
        messageEvent.setDestNetworkName(destNetworkName);
    }

    /**
     * Looks up the network name from the appropriate map based on a network type.
     * Supports Service Providers (SP), SMPP Gateways (GW), Diameter gateways, and
     * SS7 gateways.
     *
     * @param networkId   The network ID to look up
     * @param networkType The type of network ("SP" for Service Provider, "GW" for
     *                    Gateway)
     * @param protocol    The protocol of the network (SMPP, HTTP, DIAMETER, SS7)
     * @return The network name or empty string if not found
     */
    private String getNetworkName(int networkId, String networkType, String protocol) {
        log.debug("Get Network Name for: networkId={}, networkType={}, protocol={}", networkId, networkType, protocol);
        if (networkId <= 0) {
            return "";
        }
        // For Service Providers (SP)
        if ("SP".equalsIgnoreCase(networkType)) {
            return Optional.ofNullable(serviceProviders.get(networkId))
                    .map(ServiceProvider::getName)
                    .orElse("");
        }

        // For Gateways (GW) - SMPP, Diameter, or SS7
        if ("GW".equalsIgnoreCase(networkType)) {
            return switch (Optional.ofNullable(protocol).orElse("").toUpperCase()) {
                case GeneralSmscConstants.SMPP_PROTOCOL, GeneralSmscConstants.HTTP_PROTOCOL ->
                    Optional.ofNullable(gateways.get(networkId))
                            .map(Gateway::getName)
                            .orElse("");
                case GeneralSmscConstants.DIAMETER_PROTOCOL ->
                    Optional.ofNullable(diameterGatewayMap.get(networkId))
                            .map(DiameterConfig::getName)
                            .orElse("");
                case GeneralSmscConstants.SS7_PROTOCOL ->
                    Optional.ofNullable(settingsLoader.getSs7Settings(networkId))
                            .map(Ss7Settings::getName)
                            .orElse("");
                case GeneralSmscConstants.SIP_PROTOCOL ->
                        Optional.ofNullable(sipGatewaysMap.get(networkId))
                                .map(SipGateways::getName)
                                .orElse("");
                default -> {
                    log.warn("Unknown protocol {} for GW network lookup ID {}", protocol, networkId);
                    yield "";
                }
            };
        }
        return "";
    }

    private void injectServiceProviderCustomParams(MessageEvent event) {
        ServiceProvider sp = serviceProviders.get(event.getOriginNetworkId());
        if (sp == null || sp.getCustomParameters() == null || sp.getCustomParameters().isEmpty()) {
            return;
        }
        sp.getCustomParameters().forEach(event::addCustomParam);
    }

    @Async
    public void processMessage(MessageEvent messageEvent) {
        // Only check DND if USE_DND_FILTERING is enabled
        if (settingsLoader.isCommonSettingEnabled(USE_DND_FILTERING) && isBlockedByDnd(messageEvent)) {
            log.warn("Message blocked by DND filtering for destination: {}", messageEvent.getDestinationAddr());
            this.sendFailedCdr(messageEvent);
            return;
        }
        RoutingRule routing;
        RoutingRule.Destination destination;
        if (!messageEvent.isRetry()) {
            routing = this.routingMatcher.getRouting(messageEvent);
            destination = this.routingMatcher.getDestinationByRouting(routing);
        } else {
            routing = this.routingMatcher.getRoutingForAlternative(messageEvent);
            destination = this.routingMatcher.getDestinationForAlternative(routing, messageEvent);
        }

        if (Objects.isNull(routing)) {
            log.warn("Not routing found for message when origin network id is {}", messageEvent.getOriginNetworkId());
            messageEvent.setErrorCode(ErrorCodes.NOT_ROUTING);
            this.sendFailedCdr(messageEvent);
            return;
        }

        if (Objects.isNull(destination)) {
            log.warn("No destination found for routing rule with id {}", routing.getId());
            messageEvent.setErrorCode(ErrorCodes.NOT_DESTINATION);
            this.sendFailedCdr(messageEvent);
            return;
        }

        if (GeneralSmscConstants.SIP_PROTOCOL.equals(destination.getProtocol()) && !messageEvent.isSipMessageSupported()) {
            MessageRegister registerData = this.scyllaManager.getSipRegistrationByMsisdn(messageEvent.getDestinationAddr());
            if (Objects.nonNull(registerData) && !RegEventState.ACTIVE.equals(registerData.getRegEventState())) {
                registerData = null;
            }
            destination = this.routingMatcher.validateAndGetNextNonSipDestination(destination, routing.getDestination(), messageEvent, registerData);
            if (Objects.isNull(destination)) {
                log.warn("Not destination routing found for message when origin network id is {} and destination address {}", messageEvent.getOriginNetworkId(), messageEvent.getDestinationAddr());
                messageEvent.setErrorCode(ErrorCodes.NOT_DESTINATION_ADDR_REGISTERED);
                this.sendFailedCdr(messageEvent);
                return;
            }
        }

        this.processRoutingAction(routing, destination, messageEvent);
    }

    private void processRoutingAction(RoutingRule routing, RoutingRule.Destination destination, MessageEvent messageEvent) {
        if ("SP".equalsIgnoreCase(messageEvent.getOriginNetworkType())) {
            this.injectServiceProviderCustomParams(messageEvent);
        }
        processBroadcastMessageIfApply(messageEvent);
        messageEvent.setRoutingId(routing.getId());
        messageEvent.setDestNetworkId(destination.getNetworkId());
        messageEvent.setDestProtocol(destination.getProtocol());
        messageEvent.setDestNetworkType(destination.getNetworkType());
        messageEvent.setDlr("SP".equalsIgnoreCase(destination.getNetworkType()));

        // Set network names for CDR
        this.setNetworkNames(messageEvent);
        messageEvent.setApplyForRefund(routing.isApplyForRefund());
        int esmClass = Optional.ofNullable(messageEvent.getEsmClass())
                .orElse(messageEvent.getUdhLength() > 0
                        ? DEFAULT_MODE_WITH_UDH_ESM_CLASS
                        : DEFAULT_MODE_ESM_CLASS);

        if (!Objects.equals(messageEvent.getEsmClass(), esmClass)) {
            messageEvent.setEsmClass(esmClass);
        }

        this.verifyAndCompleteDLR(messageEvent, routing);

        if (GeneralSmscConstants.SS7_PROTOCOL.equalsIgnoreCase(messageEvent.getDestProtocol())) {
            setSs7PropertiesFromSettings(messageEvent);
            this.setPrefixSccpCalledAddrSRIFromTLV(messageEvent);
        }

        if (GeneralSmscConstants.DIAMETER_PROTOCOL.equalsIgnoreCase(messageEvent.getDestProtocol())) {
            setDiameterPropertiesFromSettings(messageEvent);
        }

        this.applyRoutingRulesActions(messageEvent, routing);
        this.applyRegisterDelivery(messageEvent, destination);
        messageEvent.addCustomParam(AUTO_MAP_VERSION, routing.isAutoMapVersion());
        messageEvent.addCustomParam(HAS_ACTION_ADVANCED_RULES, routing.isHasActionAdvancedRules());
        if (!routing.isAutoMapVersion() || routing.isHasActionAdvancedRules()) {
            Map<String, Object> advancedActionsMap = Converter.clasToMap(routing.getActionAdvanced());
            messageEvent.addCustomParam(advancedActionsMap);
        }
        this.processMessagePayloadTlvIfApply(messageEvent);
        if (messageEvent.applyForLongMessage()) {
            this.processCandidatesForLongMessages(messageEvent);
        }

        if (messageEvent.isDiameterCharging() && !messageEvent.isRetry()) {
            String topicByPriority = KafkaUtils.getChargingTopicPriority(messageEvent.getSmscMessagePriority());
            this.kafkaTemplate.send(topicByPriority, messageEvent.toString());
            return;
        }

        this.publishInKafkaTopic(messageEvent);
    }

    public void processDlr(MessageEvent messageEvent) {
        messageEvent.setDlr(true);

        if (Boolean.TRUE.equals(messageEvent.getCheckSubmitSmResponse())) {
            String key = messageEvent.getDeliverSmId().toUpperCase();

            Integer errorCode = Optional.ofNullable(messageEvent.getErrorCode()).orElse(0);
            String statusDlr = StaticMethods.getStatusFromMessageStateTlv(messageEvent)
                    .orElseGet(() -> Optional.ofNullable(messageEvent.getStatus()).orElse(""));
            DeliveryReceiptState state = UtilsEnum.getDeliverReceiptState(statusDlr.toUpperCase());
            DeliveryReceipt receipt = new DeliveryReceipt(messageEvent.getMessageId(), 1, 1,
                    new Date(), new Date(), state, errorCode.toString(), "");

            if (SIP_PROTOCOL.equalsIgnoreCase(messageEvent.getOriginProtocol())) {
                this.processSipDlr(messageEvent, state, receipt);
                return;
            }

            boolean isMessageSendOverSmpp = GeneralSmscConstants.SMPP_PROTOCOL.equalsIgnoreCase(messageEvent.getOriginProtocol());
            String table = isMessageSendOverSmpp ? SMPP_SUBMIT_SM_RESULT_TABLE : HTTP_SUBMIT_SM_RESULT_TABLE;
            String resultInRaw = scyllaManager.selectFromTable(table, key);
            if (Objects.isNull(resultInRaw)) {
                log.debug("SMSC received DLR for key {} but no result found in table {}", key, table);
                return;
            }

            UtilsRecords.SubmitSmResponseEvent result = Converter.stringToObject(resultInRaw, UtilsRecords.SubmitSmResponseEvent.class);
            scyllaManager.deleteFromTable(table, key);

            // Get message priority from custom param
            this.setMessagePriorityForSmppOrHttpDlr(result, messageEvent);

            this.sentRefund(state, messageEvent, result.applyForRefund(), result.submitSmServerId());
            messageEvent.setMessageId(result.submitSmServerId());
            if (isMessageSendOverSmpp) {
                if ("GW".equalsIgnoreCase(result.originNetworkType()) && GeneralSmscConstants.SMPP_PROTOCOL.equalsIgnoreCase(result.originProtocol())) {
                    log.warn("This message is ignored because origin is SMPP and network type is Gateway");
                    messageEvent.setProcess(false);
                    messageEvent.setErrorCode(ErrorCodes.NOT_SUPPORTED);
                    this.sendFailedCdr(messageEvent);
                    return;
                }
                messageEvent.setTotalSegment(result.totalSegment());
                messageEvent.setSegmentSequence(result.segmentSequence());
                messageEvent.setMsgReferenceNumber(result.msgReferenceNumber());
                messageEvent.setMessageId(result.submitSmServerId());
                handleReceiptDelivery(messageEvent, result);
            } else {
                messageEvent.setSystemId(result.systemId());
                messageEvent.setDeliverSmServerId(result.submitSmServerId());
                ESMClass dlrEsmClass = new ESMClass(MessageMode.DEFAULT, MessageType.SMSC_DEL_RECEIPT, GSMSpecificFeature.DEFAULT);
                setChangeReceiptId(receipt, result.submitSmServerId(), messageEvent);
                messageEvent.setShortMessage(receipt.toString());
                messageEvent.setDelReceipt(receipt.toString());
                messageEvent.setErrorCode(errorCode);
                messageEvent.setStatus(receipt.getFinalStatus().name());
                messageEvent.setRegisteredDelivery(RequestDelivery.NON_REQUEST_DLR.getValue());
                messageEvent.setEsmClass((int) dlrEsmClass.value());
                messageEvent.setId(System.currentTimeMillis() + "-" + System.nanoTime());
            }
            messageEvent.setDestProtocol(result.originProtocol());
            messageEvent.setDestNetworkId(result.originNetworkId());
            messageEvent.setDestNetworkType(result.originNetworkType());
            messageEvent.setParentId(result.parentId());

            // Set network names for CDR
            this.setNetworkNames(messageEvent);

            this.configureDlrMessageEventByMessageOriginProtocol(messageEvent, result);
            this.encodeSmscGeneratedDlr(messageEvent);
            this.publishInKafkaTopic(messageEvent);
        } else {
            // Set network names for CDR
            this.setNetworkNames(messageEvent);
            this.encodeSmscGeneratedDlr(messageEvent);
            this.publishDlrInKafkaTopic(messageEvent, messageEvent.getDestProtocol(), messageEvent.getDestNetworkType());
        }
    }

    private void sentRefund(DeliveryReceiptState state, MessageEvent messageEvent, boolean applyForRefund, String submitSmServerId) {
        Optional<MessageEvent> optionalForRefund = ChargingUtils.chekDeliverForRefundMessage(state, messageEvent, applyForRefund, submitSmServerId);
        optionalForRefund.ifPresent(event -> {
            String topicByPriority = KafkaUtils.getChargingTopicPriority(messageEvent.getSmscMessagePriority());
            this.kafkaTemplate.send(topicByPriority, event.toString());
        });
    }

    private void processSipDlr(MessageEvent receiveDlrEvent, DeliveryReceiptState state, DeliveryReceipt receipt) {
        String key = receiveDlrEvent.getDeliverSmId();
        String existsSubmitResp = scyllaManager.selectFromTable(SIP_SUBMIT_SM_RESULT_TABLE, key);
        if (existsSubmitResp == null || existsSubmitResp.isBlank()) {
            return;
        }

        MessageEvent originalMessageEvent = Converter.stringToObject(existsSubmitResp, MessageEvent.class);
        scyllaManager.deleteFromTable(SIP_SUBMIT_SM_RESULT_TABLE, key);

        if (!originalMessageEvent.isFinalSegmentForSplitSmsc()) {
            log.debug("Skipping SIP DLR for non-final segment: key={}, messageId={}, parentId={}, segment={}/{}, splitForSmsc={}", key,
                    originalMessageEvent.getMessageId(),
                    originalMessageEvent.getParentId(),
                    originalMessageEvent.getSegmentSequence(),
                    originalMessageEvent.getTotalSegment(),
                    originalMessageEvent.isSplitForSmsc());
            return;
        }

        this.sentRefund(state, originalMessageEvent, originalMessageEvent.isApplyForRefund(), originalMessageEvent.getMessageId());

        originalMessageEvent = originalMessageEvent.createDeliveryReceiptMessage(null, null);
        originalMessageEvent.setShortMessage(receipt.toString());
        originalMessageEvent.setDelReceipt(receipt.toString());
        originalMessageEvent.setErrorCode(receiveDlrEvent.getErrorCode());
        originalMessageEvent.setStatus(receipt.getFinalStatus().name());
        originalMessageEvent.setOptionalParameters(null);

        this.setNetworkNames(originalMessageEvent);
        this.encodeSmscGeneratedDlr(originalMessageEvent);

        this.publishDlrInKafkaTopic(originalMessageEvent, originalMessageEvent.getDestProtocol(), originalMessageEvent.getDestNetworkType());
    }

    private void setChangeReceiptId(DeliveryReceipt receipt, String id, MessageEvent event) {
        if (event.isUseProxy()) {
            receipt.setId(event.getDeliverSmId());
        } else {
            receipt.setId(id);
        }
    }
}
