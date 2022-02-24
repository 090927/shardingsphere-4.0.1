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

package org.apache.shardingsphere.encrypt.rewrite.parameter.impl;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.shardingsphere.encrypt.rewrite.parameter.EncryptParameterRewriter;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.impl.InsertSQLStatementContext;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.assignment.AssignmentSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.assignment.SetAssignmentsSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.underlying.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.underlying.rewrite.parameter.builder.impl.GroupedParameterBuilder;
import org.apache.shardingsphere.underlying.rewrite.parameter.builder.impl.StandardParameterBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Assignment parameter rewriter for encrypt.
 *
 * @author zhangliang
 */
public final class EncryptAssignmentParameterRewriter extends EncryptParameterRewriter {
    
    @Override
    protected boolean isNeedRewriteForEncrypt(final SQLStatementContext sqlStatementContext) {

        // 1、UpdateStatement、InsertSQLStatementContext
        return sqlStatementContext.getSqlStatement() instanceof UpdateStatement
                || sqlStatementContext instanceof InsertSQLStatementContext && sqlStatementContext.getSqlStatement().findSQLSegment(SetAssignmentsSegment.class).isPresent();
    }
    
    @Override
    public void rewrite(final ParameterBuilder parameterBuilder, final SQLStatementContext sqlStatementContext, final List<Object> parameters) {
        String tableName = sqlStatementContext.getTablesContext().getSingleTableName();

        // 获取 SetAssignmentsSegment 并进行遍历
        for (AssignmentSegment each : getSetAssignmentsSegment(sqlStatementContext.getSqlStatement()).getAssignments()) {

            // 判断是否存在 ShardingEncryptor
            if (each.getValue() instanceof ParameterMarkerExpressionSegment && getEncryptRule().findShardingEncryptor(tableName, each.getColumn().getName()).isPresent()) {
                StandardParameterBuilder standardParameterBuilder = parameterBuilder instanceof StandardParameterBuilder
                        ? (StandardParameterBuilder) parameterBuilder : ((GroupedParameterBuilder) parameterBuilder).getParameterBuilders().get(0);

                /**
                 * 对参数，进行加密 {@link #encryptParameters(StandardParameterBuilder, String, AssignmentSegment, List)}
                 */
                encryptParameters(standardParameterBuilder, tableName, each, parameters);
            }
        }
    }
    
    private SetAssignmentsSegment getSetAssignmentsSegment(final SQLStatement sqlStatement) {
        if (sqlStatement instanceof InsertStatement) {
            Optional<SetAssignmentsSegment> result = ((InsertStatement) sqlStatement).getSetAssignment();
            Preconditions.checkState(result.isPresent());
            return result.get();
        }
        return ((UpdateStatement) sqlStatement).getSetAssignment();
    }
    
    private void encryptParameters(final StandardParameterBuilder parameterBuilder, final String tableName, final AssignmentSegment assignmentSegment, final List<Object> parameters) {
        String columnName = assignmentSegment.getColumn().getName();
        int parameterMarkerIndex = ((ParameterMarkerExpressionSegment) assignmentSegment.getValue()).getParameterMarkerIndex();
        Object originalValue = parameters.get(parameterMarkerIndex);

        /**
         * 通过 ShardingEncryptor 进行加密，并替换原来存储密文的 cipherColumn {@link org.apache.shardingsphere.core.rule.EncryptRule#getEncryptValues(String, String, List)}
         */
        Object cipherValue = getEncryptRule().getEncryptValues(tableName, columnName, Collections.singletonList(originalValue)).iterator().next();
        parameterBuilder.addReplacedParameters(parameterMarkerIndex, cipherValue);
        Collection<Object> addedParameters = new LinkedList<>();


        // 如果存在 assistedQueryColumn，则添加辅助查询字段
        if (getEncryptRule().findAssistedQueryColumn(tableName, columnName).isPresent()) {
            Object assistedQueryValue = getEncryptRule().getEncryptAssistedQueryValues(tableName, columnName, Collections.singletonList(originalValue)).iterator().next();
            addedParameters.add(assistedQueryValue);
        }

        // 如果存在 plainColumn，则添加明文字段
        if (getEncryptRule().findPlainColumn(tableName, columnName).isPresent()) {
            addedParameters.add(originalValue);
        }
        if (!addedParameters.isEmpty()) {
            parameterBuilder.addAddedParameters(parameterMarkerIndex + 1, addedParameters);
        }
    }
}
