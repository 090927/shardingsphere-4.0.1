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

package org.apache.shardingsphere.encrypt.rewrite.context;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.rule.EncryptRule;
import org.apache.shardingsphere.encrypt.rewrite.parameter.impl.EncryptInsertValueParameterRewriter;
import org.apache.shardingsphere.encrypt.rewrite.parameter.impl.EncryptPredicateParameterRewriter;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.underlying.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.underlying.rewrite.context.SQLRewriteContextDecorator;
import org.apache.shardingsphere.encrypt.rewrite.parameter.EncryptParameterRewriterBuilder;
import org.apache.shardingsphere.encrypt.rewrite.token.EncryptTokenGenerateBuilder;
import org.apache.shardingsphere.underlying.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.underlying.rewrite.parameter.rewriter.ParameterRewriter;

import java.util.List;

/**
 * SQL rewrite context decorator for encrypt.
 * 
 * @author zhangliang
 *
 *  用于处理脱敏
 */
@RequiredArgsConstructor
public final class EncryptSQLRewriteContextDecorator implements SQLRewriteContextDecorator {
    
    private final EncryptRule encryptRule;
    
    private final boolean isQueryWithCipherColumn;
    
    @Override
    public void decorate(final SQLRewriteContext sqlRewriteContext) {

        /**
         *  改写SQL {@link EncryptParameterRewriterBuilder#getParameterRewriters()}
         */
        for (ParameterRewriter each : new EncryptParameterRewriterBuilder(encryptRule, isQueryWithCipherColumn).getParameterRewriters(sqlRewriteContext.getRelationMetas())) {

            // 判断,是否需要改写SQL
            if (!sqlRewriteContext.getParameters().isEmpty() && each.isNeedRewrite(sqlRewriteContext.getSqlStatementContext())) {

                /**
                 *  执行改写逻辑
                 *   1、{@link org.apache.shardingsphere.encrypt.rewrite.parameter.impl.EncryptAssignmentParameterRewriter#rewrite(ParameterBuilder, SQLStatementContext, List)}
                 *   2、{@link EncryptPredicateParameterRewriter#rewrite(ParameterBuilder, SQLStatementContext, List)}
                 *   3、{@link EncryptInsertValueParameterRewriter#rewrite(ParameterBuilder, SQLStatementContext, List)}
                 */
                each.rewrite(sqlRewriteContext.getParameterBuilder(), sqlRewriteContext.getSqlStatementContext(), sqlRewriteContext.getParameters());
            }
        }

        /**
         * SQLTokenGenerator 初始化
         */
        sqlRewriteContext.addSQLTokenGenerators(new EncryptTokenGenerateBuilder(encryptRule, isQueryWithCipherColumn).getSQLTokenGenerators());
    }
}
