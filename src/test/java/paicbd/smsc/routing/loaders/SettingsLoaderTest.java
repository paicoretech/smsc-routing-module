package paicbd.smsc.routing.loaders;

import com.paicbd.smsc.dto.CommonVariables;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.Ss7Settings;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.JedisCluster;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.smsc.utils.GeneralSmscConstants.GENERAL_SETTINGS_COMMON_KEY;
import static com.paicbd.smsc.utils.GeneralSmscConstants.USE_DND_FILTERING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SettingsLoaderTest {
    @Mock
    JedisCluster jedisCluster;

    @Mock
    ConcurrentMap<Integer, Ss7Settings> ss7SettingsMap;

    @Mock
    ConcurrentMap<String, String> commonSettingsMap;

    @Spy
    @InjectMocks
    SettingsLoader settingsLoader;

    @BeforeEach
    void setUp() {
        ss7SettingsMap = spy(new ConcurrentHashMap<>());
        settingsLoader = new SettingsLoader(jedisCluster, ss7SettingsMap, commonSettingsMap);
    }

    @Test
    void initWhenStringGeneralSettingsFromRedisIsNullThenGeneralSettingsIsNull() {
        when(jedisCluster.hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, GeneralSmscConstants.GENERAL_SETTINGS_SMPP_HTTP_CONFIG_KEY)).thenReturn(null);
        when(jedisCluster.hgetAll(GeneralSmscConstants.SS7_SETTINGS_HASH_NAME)).thenReturn(Collections.emptyMap());

        settingsLoader.init();

        assertEquals(0, ss7SettingsMap.size());
        assertNull(settingsLoader.getSmppHttpSettings());
    }

    @Test
    void initWhenStringGeneralSettingsFromRedisIsNotNullThenGeneralSettingsIsNotNull() {
        GeneralSettings generalSettings = GeneralSettings.builder()
                .id(1)
                .validityPeriod(120)
                .maxValidityPeriod(600)
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .encodingIso88591(1)
                .encodingGsm7(1)
                .encodingUcs2(1)
                .build();

        Ss7Settings ss7Settings = Ss7Settings.builder()
                .networkId(1)
                .name("fakeSs7Settings")
                .mnoId(1)
                .splitMessage(true)
                .globalTitle("GT100")
                .build();

        when(jedisCluster.hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, GeneralSmscConstants.GENERAL_SETTINGS_SMPP_HTTP_CONFIG_KEY)).thenReturn(generalSettings.toString());
        when(jedisCluster.hgetAll(GeneralSmscConstants.SS7_SETTINGS_HASH_NAME)).thenReturn(Collections.singletonMap("1", ss7Settings.toString()));

        settingsLoader.init();

        assertEquals(1, ss7SettingsMap.size());
        assertEquals(generalSettings.toString(), settingsLoader.getSmppHttpSettings().toString());
    }

    @Test
    void updateSpecificSs7SettingWhenSs7SettingIsNullThenDoNothing() {
        when(jedisCluster.hget(GeneralSmscConstants.SS7_SETTINGS_HASH_NAME, "1")).thenReturn(null);
        settingsLoader.updateSpecificSs7Setting(1);
        verify(ss7SettingsMap, never()).put(1, null);
        assertNull(ss7SettingsMap.get(1));
    }

    @Test
    void updateSpecificSs7SettingWhenSs7SettingIsNotNullThenUpdateSs7Setting() {
        Ss7Settings ss7Settings = Ss7Settings.builder()
                .networkId(1)
                .name("fakeSs7Settings")
                .mnoId(1)
                .splitMessage(true)
                .globalTitle("GT100")
                .build();

        when(jedisCluster.hget(GeneralSmscConstants.SS7_SETTINGS_HASH_NAME, "1")).thenReturn(ss7Settings.toString());
        settingsLoader.updateSpecificSs7Setting(1);

        ArgumentCaptor<Integer> networkIdCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Ss7Settings> ss7SettingsCaptor = ArgumentCaptor.forClass(Ss7Settings.class);

        verify(ss7SettingsMap).put(networkIdCaptor.capture(), ss7SettingsCaptor.capture());
        assertEquals(1, ss7SettingsMap.size());
        assertEquals(1, (int) networkIdCaptor.getValue());
        assertEquals(ss7Settings.toString(), ss7SettingsCaptor.getValue().toString());

        assertEquals(settingsLoader.getSs7Settings(1).toString(), ss7Settings.toString());
    }

    @Test
    void removeFromSs7MapWhenNetworkIdIsNotNullThenRemoveSs7Setting() {
        Ss7Settings ss7Settings = Ss7Settings.builder()
                .networkId(1)
                .name("fakeSs7Settings")
                .mnoId(1)
                .splitMessage(true)
                .globalTitle("GT100")
                .build();
        ss7SettingsMap.put(1, ss7Settings);

        settingsLoader.removeFromSs7Map(1);
        verify(ss7SettingsMap).remove(1);
        assertEquals(0, ss7SettingsMap.size());
    }

    @ParameterizedTest
    @CsvSource({"true", "false"})
    @DisplayName("Load or update common settings when data exists and not exists in Redis")
    void loadOrUpdateCommonSettingsTestWhenExistAndNotExistsDataInRedisTheDoSuccessfully(boolean existDataInRedis) {
        CommonVariables commonVariables = new CommonVariables();
        commonVariables.setKey(USE_DND_FILTERING);
        commonVariables.setDataType("boolean");
        commonVariables.setValue("true");

        List<CommonVariables> commonVariablesList = List.of(commonVariables);
        String commonSettingsRaw = Converter.valueAsString(commonVariablesList);

        if (existDataInRedis) {
            when(jedisCluster.hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, GENERAL_SETTINGS_COMMON_KEY)).thenReturn(commonSettingsRaw);
        }

        settingsLoader.loadOrUpdateCommonSettings();

        if (existDataInRedis) {
            ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
            verify(commonSettingsMap).put(key.capture(), value.capture());
            assertEquals(commonVariables.getValue(), value.getValue());
            assertEquals(commonVariables.getKey(), key.getValue());
        } else {
            verifyNoMoreInteractions(commonSettingsMap);
        }
    }

    @ParameterizedTest
    @CsvSource({"USE_DND_FILTERING", "USE_CHARGING"})
    @DisplayName("Common setting enabled when data exists and not exists in the Map")
    void isCommonSettingEnabledTestWhenKeyExistsAndNotExistsDataInMapThenDoSuccessfully(String key) {

        ConcurrentMap<String, String> commonSettingsMapReal = new ConcurrentHashMap<>();
        commonSettingsMapReal.put(USE_DND_FILTERING, "true");

        settingsLoader = new SettingsLoader(jedisCluster, ss7SettingsMap, commonSettingsMapReal);
        boolean isEnabled = settingsLoader.isCommonSettingEnabled(key);

        if (USE_DND_FILTERING.equals(key)) {
            assertTrue(isEnabled);
        } else {
            assertFalse(isEnabled);
        }
    }
}