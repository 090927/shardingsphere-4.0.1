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

package org.apache.shardingsphere.underlying.rewrite.sql.token.generator;

import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;

/**
 * SQL token generator.
 *
 * @author zhangliang
 *
 *  作用: 专门负责生产具体的 Token
 *
 *   子类：
 *     生成单个 SQLToken {@link OptionalSQLTokenGenerator}
 *     批量生成 SQLToken {@link CollectionSQLTokenGenerator}
 */
public interface SQLTokenGenerator {
    
    /**
     * Judge is generate SQL token or not.
     *
     * @param sqlStatementContext SQL statement context
     * @return is generate SQL token or not
     *
     *  判断是否要生成 SQLToken
     */
    boolean isGenerateSQLToken(SQLStatementContext sqlStatementContext);
}
