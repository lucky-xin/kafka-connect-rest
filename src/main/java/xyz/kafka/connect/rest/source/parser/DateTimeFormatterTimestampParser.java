package xyz.kafka.connect.rest.source.parser;
/*
 *            Copyright © 2024 chaoxin.lu
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * DateTimeFormatterTimestampParser
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class DateTimeFormatterTimestampParser implements TimestampParser {

    private static final String ITEM_TIMESTAMP_PATTERN = "rest.response.record.timestamp.parser.pattern";
    private static final String ITEM_TIMESTAMP_ZONE = "rest.response.record.timestamp.parser.zone";
    private DateTimeFormatter timestampFormatter;
    public static ConfigDef config() {
        return new ConfigDef()
                .define(ITEM_TIMESTAMP_PATTERN, STRING, "yyyy-MM-dd'T'HH:mm:ss[.SSS]X", LOW, "Timestamp format pattern")
                .define(ITEM_TIMESTAMP_ZONE, STRING, "UTC", LOW, "Timestamp ZoneId");
    }
    @Override
    public void configure(Map<String, ?> settings) {
        SimpleConfig simpleConfig = new SimpleConfig(config(),settings);
        timestampFormatter = DateTimeFormatter.ofPattern(simpleConfig.getString(ITEM_TIMESTAMP_PATTERN))
                .withZone(ZoneId.of(simpleConfig.getString(ITEM_TIMESTAMP_ZONE)));
    }

    @Override
    public Instant parse(String timestamp) {
        return OffsetDateTime.parse(timestamp, timestampFormatter).toInstant();
    }
}
