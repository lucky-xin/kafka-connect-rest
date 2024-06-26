package xyz.kafka.connect.rest.sink.writer;
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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.writer.ObjectWriter;
import org.apache.commons.text.StringSubstitutor;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * FastJsonWriter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-08-15
 */
@SuppressWarnings("all")
public class FastJsonWriter implements ObjectWriter<Map<String, Object>> {

    private static final Logger log = LoggerFactory.getLogger(FastJsonWriter.class);

    private Optional<String> bodyTemplate;

    public FastJsonWriter(Optional<String> bodyTemplate) {
        this.bodyTemplate = bodyTemplate;
    }

    @Override
    public void write(JSONWriter writer, Object object, Object fieldName, Type fieldType, long features) {
        if (this.bodyTemplate.isEmpty()) {
            writer.write((Map<String, Object>) object);
            return;
        }
        String result = null;
        try {
            String jsonTemplate = bodyTemplate.get();
            StringSubstitutor sub = new StringSubstitutor((Map<String, Object>) object);
            result = sub.replace(jsonTemplate);
            writer.writeBinary(result.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("parse object error:" + result, e);
            log.error("object:{}", JSON.toJSONString(object));
            throw new ConnectException(e);
        }
    }
}
