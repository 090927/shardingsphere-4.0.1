/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.sql.parser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SQL parse engine factory.
 *
 * @author zhangliang
 *
 * SQL 解析引擎,工厂类
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SQLParseEngineFactory {
    
    private static final Map<String, SQLParseEngine> ENGINES = new ConcurrentHashMap<>();
    
    /**
     * Get SQL parse engine.
     *
     * @param databaseTypeName name of database type
     * @return SQL parse engine
     *
     *  基于数据类类型，获取 SQL解析引擎
     */
    public static SQLParseEngine getSQLParseEngine(final String databaseTypeName) {

        // 如果缓存中包含了指定数据库类型对应的 SQLParseEngine，则直接返回
        if (ENGINES.containsKey(databaseTypeName)) {
            return ENGINES.get(databaseTypeName);
        }
        synchronized (ENGINES) {
            if (ENGINES.containsKey(databaseTypeName)) {
                return ENGINES.get(databaseTypeName);
            }

            /**
             * 创建 SQLParseEngine {@link SQLParseEngine
             */
            SQLParseEngine result = new SQLParseEngine(databaseTypeName);

            // 将新创建的 SQLParseEngine 放入缓存中
            ENGINES.put(databaseTypeName, result);
            return result;
        }
    }
}
