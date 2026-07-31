package paicbd.smsc.routing.util;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.UtilsEnum;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.bean.OptionalParameter;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.paicbd.smsc.utils.EncodingUtils.GSM7_DATA_CODINGS;
import static com.paicbd.smsc.utils.EncodingUtils.UCS2_DATA_CODINGS;
import static com.paicbd.smsc.utils.GeneralSmscConstants.DIAMETER_PROTOCOL;
import static com.paicbd.smsc.utils.GeneralSmscConstants.HTTP_PROTOCOL;
import static com.paicbd.smsc.utils.GeneralSmscConstants.SMPP_PROTOCOL;
import static com.paicbd.smsc.utils.GeneralSmscConstants.SS7_PROTOCOL;

@Slf4j
@UtilityClass
public class StaticMethods {
    private static final Pattern pattern = Pattern.compile("\\{\\{([^}]+)}}");

    private static final int DEFAULT_GSM7_SMS_LENGTH = 160;
    private static final int DEFAULT_UCS2_SMS_LENGTH = 70;
    private static final int DEFAULT_BINARY_SMS_LENGTH = 140;

    public static UtilsEnum.MessageType getMessageType(MessageEvent event) {
        UtilsEnum.MessageType messageType;
        switch (event.getOriginProtocol()) {
            case SS7_PROTOCOL -> {
                if (Objects.isNull(event.getImsi())) {
                    messageType = UtilsEnum.MessageType.MESSAGE;
                } else {
                    messageType = (event.isMoMessage()) ? UtilsEnum.MessageType.DELIVER : UtilsEnum.MessageType.MESSAGE;
                }
            }
            case HTTP_PROTOCOL, SMPP_PROTOCOL, DIAMETER_PROTOCOL ->
                    messageType = (event.isDlr()) ? UtilsEnum.MessageType.DELIVER : UtilsEnum.MessageType.MESSAGE;
            default -> throw new IllegalStateException("Unexpected value: " + event.getOriginProtocol());
        }
        return messageType;
    }

    public static List<String> extractRegexFromMessage(String newMessage) {
        List<String> regexList = new ArrayList<>();
        Pattern pattern = Pattern.compile("__(.*?)__");
        Matcher matcher = pattern.matcher(newMessage);

        while (matcher.find()) {
            String extracted = matcher.group(1);
            extracted = extracted.replace("\\\\", "\\");
            regexList.add(extracted);
        }

        return regexList;
    }

    public static List<String> applyRegexToInput(String input, List<String> regexList) {
        List<String> matches = new ArrayList<>();

        for (String regex : regexList) {
            try {
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(input);

                if (matcher.find()) {
                    matches.add(matcher.group(matcher.groupCount() > 0 ? 1 : 0));
                } else {
                    log.warn("No match found for regex: {}. Adding empty string", regex);
                    matches.add("");
                }
            } catch (PatternSyntaxException e) {
                log.error("Error while applying regex to input: {}, setting empty string", e.getMessage());
                matches.add("");
            }
        }

        return matches;
    }

    public static String buildOutput(String newMessage, List<String> matches) {
        StringBuilder output = new StringBuilder();
        Pattern pattern = Pattern.compile("__.*?__");
        Matcher matcher = pattern.matcher(newMessage);

        int index = 0;
        while (matcher.find() && index < matches.size()) {
            matcher.appendReplacement(output, Matcher.quoteReplacement(matches.get(index)));
            index++;
        }
        matcher.appendTail(output);
        return output.toString();
    }

    public static Optional<String> processReplaceAction(String template, MessageEvent messageEvent) {
        Matcher matcher = pattern.matcher(template);
        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            String propertyName = matcher.group(1).trim();
            String replacement = getPropertyFromMessageEvent(messageEvent, propertyName);
            if (Objects.isNull(replacement)) {
                log.warn("No replacement found for property: {}", propertyName);
                return Optional.empty();
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }

        matcher.appendTail(result);
        return Optional.of(result.toString());
    }

    private static String getPropertyFromMessageEvent(MessageEvent messageEvent, String propertyName) {
        try {
            for (PropertyDescriptor pd : Introspector.getBeanInfo(messageEvent.getClass()).getPropertyDescriptors()) {
                if (pd.getName().equals(propertyName)) {
                    Method getter = pd.getReadMethod();
                    if (getter != null) {
                        Object value = getter.invoke(messageEvent);
                        return value != null ? value.toString() : "";
                    }
                    log.warn("No getter found for property: {}", propertyName);
                }
            }
        } catch (Exception e) {
            log.error("Error while getting property from message event: {}", e.getMessage());
        }
        return null;
    }

    public static Integer obtainSs7SmsLengthByDataCoding(MessageEvent event, boolean shouldSplitMessage, int concatUdhLength) {
        int dataCoding = event.getDataCoding();
        if (GSM7_DATA_CODINGS.contains(dataCoding)) {
            log.debug("To ss7, Data coding: {}. Using GSM 7-bit rules.", dataCoding);
            return shouldSplitMessage ? calculateGsm7Length(event, concatUdhLength) : DEFAULT_GSM7_SMS_LENGTH;
        }

        if (UCS2_DATA_CODINGS.contains(dataCoding)) {
            log.debug("To ss7, Data coding: {}. Using UCS2 rules.", dataCoding);
            return shouldSplitMessage ? calculateSs7Ucs2Length(event, concatUdhLength) : DEFAULT_UCS2_SMS_LENGTH;
        }

        log.debug("To ss7, Data coding: {}. Using binary rules.", dataCoding);
        return shouldSplitMessage ? calculateBinaryLength(event, concatUdhLength) : DEFAULT_BINARY_SMS_LENGTH;
    }

    private static int calculateGsm7Length(MessageEvent event, int concatUdhLength) {
        int messageLength = EncodingUtils.isHexadecimal(event.getShortMessage()) ?
                (event.getShortMessage().length() / 2) : event.getShortMessage().length();

        if (event.getUdhLength() > 0) {
            int udhTotalBytes = event.getUdhLength() + 1;
            int udhBits = udhTotalBytes * 8;
            int udhChars = (int) Math.ceil(udhBits / 7.0);
            int availableChars = DEFAULT_GSM7_SMS_LENGTH - udhChars;

            if (messageLength <= availableChars) {
                return availableChars;
            } else {
                int totalUdhBytes = udhTotalBytes + concatUdhLength;
                int totalUdhBits = totalUdhBytes * 8;
                int totalUdhChars = (int) Math.ceil(totalUdhBits / 7.0);
                return DEFAULT_GSM7_SMS_LENGTH - totalUdhChars;
            }

        } else if (messageLength > DEFAULT_GSM7_SMS_LENGTH) {
            return DEFAULT_GSM7_SMS_LENGTH - (int) Math.ceil(concatUdhLength * 8 / 7.0);
        }

        return DEFAULT_GSM7_SMS_LENGTH;
    }

    private static int calculateSs7Ucs2Length(MessageEvent event, int concatUdhLength) {
        int realLength = event.getShortMessage().length();
        int udhLength = event.getUdhLength();

        int currentUdhChars = udhLength > 0
                ? (int) Math.ceil((udhLength + 1) / 2.0)
                : 0;

        if ((realLength + currentUdhChars) > DEFAULT_UCS2_SMS_LENGTH) {
            return DEFAULT_UCS2_SMS_LENGTH
                   - (int) Math.ceil(concatUdhLength / 2.0)
                   - (int) Math.ceil(udhLength / 2.0);
        }

        return DEFAULT_UCS2_SMS_LENGTH - currentUdhChars;
    }

    private static int calculateBinaryLength(MessageEvent event, int concatUdhLength) {
        boolean isHex = EncodingUtils.isHexadecimal(event.getShortMessage().trim());
        int realLength = isHex ? event.getShortMessage().length() / 2 : event.getShortMessage().length();
        int udhLength = event.getUdhLength();

        int totalUdhBytes = udhLength > 0 ? udhLength + 1 : 0;

        if ((realLength + totalUdhBytes) > DEFAULT_BINARY_SMS_LENGTH) {
            return DEFAULT_BINARY_SMS_LENGTH
                   - concatUdhLength
                   - udhLength;
        }

        return DEFAULT_BINARY_SMS_LENGTH - totalUdhBytes;
    }

    public static Optional<String> getStatusFromMessageStateTlv(MessageEvent messageEvent) {
        if (messageEvent.getOptionalParameters() == null) {
            return Optional.empty();
        }
        return messageEvent.getOptionalParameters().stream()
                .filter(tlv -> tlv.tag() == OptionalParameter.Tag.MESSAGE_STATE.code())
                .findFirst()
                .map(tlv -> {
                    int messageStateValue = Integer.parseInt(tlv.value());
                    return mapMessageStateToDeliveryStatus(messageStateValue);
                });
    }

    public static String mapMessageStateToDeliveryStatus(int messageStateValue) {
        return switch (messageStateValue) {
            case 1 -> "ENROUTE";
            case 2 -> "DELIVRD";
            case 3 -> "EXPIRED";
            case 4 -> "DELETED";
            case 5 -> "UNDELIV";
            case 6 -> "ACCEPTD";
            case 8 -> "REJECTD";
            default -> "UNKNOWN";
        };
    }
}
