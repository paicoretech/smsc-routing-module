package paicbd.smsc.routing.loaders;

import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.CommonVariables;
import com.paicbd.smsc.utils.Converter;
import com.fasterxml.jackson.core.type.TypeReference;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.paicbd.smsc.dto.Ss7Settings;
import org.springframework.util.Assert;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.smsc.utils.GeneralSmscConstants.GENERAL_SETTINGS_COMMON_KEY;
import static com.paicbd.smsc.utils.GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME;
import static com.paicbd.smsc.utils.GeneralSmscConstants.GENERAL_SETTINGS_SMPP_HTTP_CONFIG_KEY;
import static com.paicbd.smsc.utils.GeneralSmscConstants.SS7_SETTINGS_HASH_NAME;

@Slf4j
@Component
@RequiredArgsConstructor
public class SettingsLoader {
    private final JedisCluster jedisCluster;
    private final ConcurrentMap<Integer, Ss7Settings> ss7SettingsMap;
    private final ConcurrentMap<String, String> commonSettingsMap;

    @Getter
    private GeneralSettings smppHttpSettings;

    @PostConstruct
    public void init() {
        log.info("Loading Global Settings");
        this.loadOrUpdateSmppHttpSettings();
        this.loadAllSs7Settings();
    }

    public void loadOrUpdateSmppHttpSettings() {
        log.info("Start loading smpp-http settings");
        String gsRaw = this.jedisCluster.hget(GENERAL_SETTINGS_HASH_NAME,
                GENERAL_SETTINGS_SMPP_HTTP_CONFIG_KEY);

        if (Objects.isNull(gsRaw)) {
            log.error("Error loading smpp-http settings for protocol key: {}", GENERAL_SETTINGS_SMPP_HTTP_CONFIG_KEY);
            return;
        }

        this.smppHttpSettings = Converter.stringToObject(gsRaw, GeneralSettings.class);
    }

    private void loadAllSs7Settings() {
        log.info("Start loading ss7 settings");
        Map<String, String> hashValues = this.jedisCluster.hgetAll(SS7_SETTINGS_HASH_NAME);
        if (hashValues.isEmpty()) {
            log.warn("No ss7 settings found in cache.");
            return;
        }

        hashValues.forEach((ss7GatewayNetworkId, data) -> {
            Ss7Settings ss7Setting = Converter.stringToObject(data, Ss7Settings.class);
            Assert.notNull(ss7Setting, "An error occurred while casting Ss7Settings in loadAllSs7Settings");
            this.addToSs7Map(Integer.parseInt(ss7GatewayNetworkId), ss7Setting);
        });

        log.info("Loaded ss7 settings, settings loaded: {}", ss7SettingsMap.size());
    }

    public void addToSs7Map(int networkId, Ss7Settings ss7Settings) {
        ss7SettingsMap.put(networkId, ss7Settings);
        log.info("Ss7 settings for network id {} was added to cache: {}", networkId, ss7Settings.toString());
    }

    public Ss7Settings getSs7Settings(Integer networkId) {
        return ss7SettingsMap.get(networkId);
    }

    public void updateSpecificSs7Setting(Integer networkId) {
        String ss7Setting = jedisCluster.hget(SS7_SETTINGS_HASH_NAME, networkId.toString());
        if (Objects.isNull(ss7Setting)) {
            log.info("Error trying update ss7 setting for network id {}", networkId);
            return;
        }
        Ss7Settings ss7SettingsToUpdate = Converter.stringToObject(ss7Setting, Ss7Settings.class);
        this.ss7SettingsMap.put(networkId, ss7SettingsToUpdate);
        log.info("Updated ss7 settings for network id {}: {}", networkId, ss7SettingsToUpdate);
    }

    public void removeFromSs7Map(int networkId) {
        ss7SettingsMap.remove(networkId);
        log.warn("Ss7 settings for network id {} was removed.", networkId);
    }

    public void loadOrUpdateCommonSettings() {
        String commonSettingsRaw = jedisCluster.hget(GENERAL_SETTINGS_HASH_NAME, GENERAL_SETTINGS_COMMON_KEY);
        if (Objects.isNull(commonSettingsRaw)) {
            log.warn("No common_settings found in Redis");
            return;
        }
        
        List<CommonVariables> commonVariables = Converter.stringToObject(commonSettingsRaw, new TypeReference<>() {
        });
        commonSettingsMap.clear();
        for (CommonVariables variable : commonVariables) {
            commonSettingsMap.put(variable.getKey(), variable.getValue());
        }
        log.info("Loaded {} common settings", commonVariables.size());
    }

    public boolean isCommonSettingEnabled(String key) {
        String value = commonSettingsMap.getOrDefault(key, "false");
        return Boolean.parseBoolean(value);
    }
}
