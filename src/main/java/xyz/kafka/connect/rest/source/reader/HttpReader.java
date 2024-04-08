package xyz.kafka.connect.rest.source.reader;
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

import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

/**
 * HttpReader
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public interface HttpReader {
    /**
     * 拉取数据
     *
     * @param offset 位移
     * @return
     */
    List<SourceRecord> poll(Map<String, Object> offset);
}
