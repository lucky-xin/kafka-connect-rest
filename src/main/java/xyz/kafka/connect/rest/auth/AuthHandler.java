package xyz.kafka.connect.rest.auth;
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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpRequest;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 认证处理器
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-07-20
 */
public interface AuthHandler {


    /**
     * 创建并返回一个缓存对象。该缓存使用Caffeine作为其底层实现，具有以下特点：
     * - 写入后1小时自动过期。
     * - 最大容纳1000个元素。
     * - 使用软引用作为值，以利于垃圾回收。
     *
     * @return Cache<String, Header> 返回一个字符串到Header的缓存映射。
     */
    default Cache<String, Header> cache() {
        return Caffeine.newBuilder()
                // 创建Caffeine缓存构建器
                .expireAfterWrite(1, TimeUnit.HOURS)
                // 设置缓存过期时间为1秒
                .maximumSize(1000)
                // 设置缓存的最大大小为1000
                .softValues()
                // 设置缓存值使用软引用，以支持垃圾回收
                .build(); // 构建并返回缓存实例
    }

    /**
     * 设置认证头
     *
     * @param req 请求
     */
    default void setAuthentication(HttpRequest req) {
        Header header = cache().get(req.getRequestUri(), k -> {
            try {
                return this.createAuthorizationHeader();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        if (header != null) {
            req.addHeader(header);
        }
    }

    /**
     * 这个函数是一个Java接口中的默认方法，它的功能是返回一个Header对象，该对象表示授权头部值。
     * 如果该方法没有被接口的实现类重写，那么它会返回null。如果没有抛出异常，该方法会返回null。
     *
     * @return Header
     * @throws IOException
     */
    Header createAuthorizationHeader() throws IOException;
}
