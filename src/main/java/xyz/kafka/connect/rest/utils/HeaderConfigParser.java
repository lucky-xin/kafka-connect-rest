package xyz.kafka.connect.rest.utils;


import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


/**
 * HeaderConfigParser
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class HeaderConfigParser {
    private static final char HTTP_HEADER_KV_SEPARATOR = ':';
    private static final HeaderConfigParser INSTANCE = new HeaderConfigParser();
    private final Splitter headerKeyValueSplitter = Splitter.on(':').limit(2).trimResults();
    private final Cache<String, Splitter> splitterCache = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .maximumSize(2)
            .softValues()
            .build(key -> Splitter.on(key).omitEmptyStrings().trimResults());

    private HeaderConfigParser() {
    }

    public static HeaderConfigParser getInstance() {
        return INSTANCE;
    }

    public List<Header> parseHeadersConfig(String configKey, String headers, String separator) throws ConfigException {
        ImmutableList.Builder<Header> headerBuilder = ImmutableList.builder();
        Splitter splitter = this.splitterCache.get(separator, k -> Splitter.on(k).omitEmptyStrings().trimResults());
        for (String headerKeyValueString : splitter.split(headers)) {
            headerBuilder.add(parseHeaderKeyValueString(headerKeyValueString)
                    .orElseThrow(() -> new ConfigException(configKey, headers)));
        }
        return headerBuilder.build();
    }

    private Optional<Header> parseHeaderKeyValueString(String headerKeyValue) {
        if (!StringUtils.contains(headerKeyValue, HTTP_HEADER_KV_SEPARATOR)) {
            return Optional.empty();
        }
        List<String> keyValueList = this.headerKeyValueSplitter.splitToList(headerKeyValue);
        if (keyValueList.size() != 2 || StringUtils.isAnyEmpty(keyValueList.get(0), keyValueList.get(1))) {
            return Optional.empty();
        }
        return Optional.of(new BasicHeader(keyValueList.get(0), keyValueList.get(1)));
    }
}
