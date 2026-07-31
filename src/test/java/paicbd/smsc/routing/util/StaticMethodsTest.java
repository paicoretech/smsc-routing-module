package paicbd.smsc.routing.util;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.utils.UtilsEnum;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StaticMethodsTest {
    @Test
    void testConstructorIsPrivate() throws NoSuchMethodException {
        Constructor<StaticMethods> constructor = StaticMethods.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        assertThrows(InvocationTargetException.class, constructor::newInstance);
    }

    @Test
    void getMessageType() {
        MessageEvent messageEvent = new MessageEvent();
        messageEvent.setOriginProtocol("SS7");
        messageEvent.setImsi(null);
        messageEvent.setMoMessage(true);
        assertEquals(UtilsEnum.MessageType.MESSAGE, StaticMethods.getMessageType(messageEvent));

        messageEvent.setImsi("100223311");
        assertEquals(UtilsEnum.MessageType.DELIVER, StaticMethods.getMessageType(messageEvent));

        messageEvent.setMoMessage(false);
        assertEquals(UtilsEnum.MessageType.MESSAGE, StaticMethods.getMessageType(messageEvent));


        messageEvent.setMoMessage(false);
        messageEvent.setOriginProtocol("HTTP");
        messageEvent.setDlr(true);
        assertEquals(UtilsEnum.MessageType.DELIVER, StaticMethods.getMessageType(messageEvent));

        messageEvent.setMoMessage(true);
        assertEquals(UtilsEnum.MessageType.DELIVER, StaticMethods.getMessageType(messageEvent));


        messageEvent.setDlr(false);
        messageEvent.setOriginProtocol("SMPP");
        assertEquals(UtilsEnum.MessageType.MESSAGE, StaticMethods.getMessageType(messageEvent));

        messageEvent.setMoMessage(true);
        assertEquals(UtilsEnum.MessageType.MESSAGE, StaticMethods.getMessageType(messageEvent));

        messageEvent.setOriginProtocol("UNKNOWN");
        assertThrows(IllegalStateException.class, () -> StaticMethods.getMessageType(messageEvent));
    }

    @Test
    void extractRegexFromMessageWithTwoRegex() {
        String shortMessage = "Your OTP for the Facebook platform is: 909102";
        String newMessage = "__(?:\\S+\\s+){4}(\\S+)__ OTP: __\\b\\d{6}\\b__"; // In the GUI: __(?:\S+\s+){4}(\S+)__ OTP: __\b\d{6}\b__

        List<String> regexList = StaticMethods.extractRegexFromMessage(newMessage);
        System.out.println(">> " + regexList);
        List<String> matches = StaticMethods.applyRegexToInput(shortMessage, regexList);
        String result = StaticMethods.buildOutput(newMessage, matches);

        assertEquals("Facebook OTP: 909102", result);
        System.out.println(result);
    }

    @Test
    void extractRegexFromMessageWithThreeRegex() {
        String shortMessage = "Google INC. Email: google@google.com, Phone: +1234567890, Amount: 1000 USD. Product: Google Pixel 5";
        String newMessage = "PRODUCT: __(?<=Product:\\s)[\\w\\s\\d]+__ AMOUNT: __(\\d+)(?=\\sUSD)__ PHONE: __\\+(\\d{10})__ EMAIL: __[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}__"; // In the GUI: PRODUCT: __(?<=Product:\s)[\w\s\d]+__ AMOUNT: __(\d+)(?=\sUSD)__ PHONE: __\+(\d{10})__ EMAIL: __[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}__

        List<String> regexList = StaticMethods.extractRegexFromMessage(newMessage);
        System.out.println(">> " + regexList);

        List<String> matches = StaticMethods.applyRegexToInput(shortMessage, regexList);
        String result = StaticMethods.buildOutput(newMessage, matches);

        assertEquals("PRODUCT: Google Pixel 5 AMOUNT: 1000 PHONE: 1234567890 EMAIL: google@google.com", result);
    }

    @Test
    void extractRegexFromMessageWithNothingMatch() {
        String shortMessage = "Hi from SMSC";
        String newMessage = "__(?:\\S+\\s+){4}(\\S+)__ OTP: __\\b\\d{6}\\b__";

        List<String> regexList = StaticMethods.extractRegexFromMessage(newMessage);
        System.out.println(">> " + regexList);

        List<String> matches = StaticMethods.applyRegexToInput(shortMessage, regexList);
        assertEquals(2, matches.size());
        String result = StaticMethods.buildOutput(newMessage, matches);

        assertEquals(" OTP: ", result);
    }


    @Test
    void processReplaceActionWithSingleProperty() {
        String newGtSccpAddrForRule = "{{destinationAddr}}";
        MessageEvent messageEvent = MessageEvent.builder()
                .sourceAddr("50587878784")
                .sourceAddrNpi(1)
                .sourceAddrNpi(1)
                .destinationAddr("50587878787")
                .destAddrNpi(1)
                .destAddrTon(1)
                .globalTitle("5058888888")
                .build();
        Optional<String> newGtSccpAddr = StaticMethods.processReplaceAction(newGtSccpAddrForRule, messageEvent);
        assertTrue(newGtSccpAddr.isPresent());
        assertEquals(messageEvent.getDestinationAddr(), newGtSccpAddr.get());
    }

    @Test
    void processReplaceActionWithMultiProperties() {
        String newGtSccpAddrForRule = "{{sourceAddr}}-{{destinationAddr}}";
        MessageEvent messageEvent = MessageEvent.builder()
                .sourceAddr("50587878784")
                .sourceAddrNpi(1)
                .sourceAddrNpi(1)
                .destinationAddr("50587878787")
                .destAddrNpi(1)
                .destAddrTon(1)
                .globalTitle("5058888888")
                .build();
        Optional<String> newGtSccpAddr = StaticMethods.processReplaceAction(newGtSccpAddrForRule, messageEvent);
        assertTrue(newGtSccpAddr.isPresent());
        assertEquals(messageEvent.getSourceAddr() + "-" + messageEvent.getDestinationAddr(), newGtSccpAddr.get());
    }


    @Test
    void processReplaceActionWithOnlyText() {
        String newGtSccpAddrForRule = "5057777777";
        MessageEvent messageEvent = MessageEvent.builder()
                .sourceAddr("50587878784")
                .sourceAddrNpi(1)
                .sourceAddrNpi(1)
                .destinationAddr("50587878787")
                .destAddrNpi(1)
                .destAddrTon(1)
                .globalTitle("5058888888")
                .build();
        Optional<String> newGtSccpAddr = StaticMethods.processReplaceAction(newGtSccpAddrForRule, messageEvent);
        assertTrue(newGtSccpAddr.isPresent());
        assertEquals(newGtSccpAddrForRule, newGtSccpAddr.get());
    }

    @Test
    void processReplaceActionWithWrongProperty() {
        String newGtSccpAddrForRule = "{{sourceAddrd}} - {{sourceAddrd}}";
        MessageEvent messageEvent = MessageEvent.builder()
                .sourceAddr("50587878784")
                .sourceAddrNpi(1)
                .sourceAddrNpi(1)
                .destinationAddr("50587878787")
                .destAddrNpi(1)
                .destAddrTon(1)
                .globalTitle("5058888888")
                .build();
        Optional<String> newGtSccpAddr = StaticMethods.processReplaceAction(newGtSccpAddrForRule, messageEvent);
        assertTrue(newGtSccpAddr.isEmpty());
    }
}