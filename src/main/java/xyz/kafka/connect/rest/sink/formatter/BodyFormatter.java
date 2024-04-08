package xyz.kafka.connect.rest.sink.formatter;
/*
 *Copyright © 2024 chaoxin.lu
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

import java.util.List;
import java.util.Map;

/**
 * BodyFormatter
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
@FunctionalInterface
public interface BodyFormatter<T> {
    /**
     * 格式化批量更新请求体
     *
     * @param records
     * @return
     */
    String formatUpdate(List<T> records);

    /**
     * 格式化批量删除请求体
     *
     * @param records
     * @return
     */
    default String formatDelete(List<T> records) {
        return JSON.toJSONString(Map.of("keys", records));
    }

}
