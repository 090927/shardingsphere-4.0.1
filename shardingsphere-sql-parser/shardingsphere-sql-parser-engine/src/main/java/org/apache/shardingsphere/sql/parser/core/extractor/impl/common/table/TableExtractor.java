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

package org.apache.shardingsphere.sql.parser.core.extractor.impl.common.table;

import com.google.common.base.Optional;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.shardingsphere.sql.parser.core.extractor.api.OptionalSQLSegmentExtractor;
import org.apache.shardingsphere.sql.parser.core.extractor.util.ExtractorUtils;
import org.apache.shardingsphere.sql.parser.core.extractor.util.RuleName;
import org.apache.shardingsphere.sql.parser.sql.segment.generic.SchemaSegment;
import org.apache.shardingsphere.sql.parser.sql.segment.generic.TableSegment;

import java.util.Map;

/**
 *  Table extractor.
 *
 * @author duhongjun
 * @author panjuan
 */
public final class TableExtractor implements OptionalSQLSegmentExtractor {
    
    @Override
    public Optional<TableSegment> extract(final ParserRuleContext ancestorNode, final Map<ParserRuleContext, Integer> parameterMarkerIndexes) {

        // 从 Context 中获取 TableName 节
        Optional<ParserRuleContext> tableNameNode = ExtractorUtils.findFirstChildNode(ancestorNode, RuleName.TABLE_NAME);
        if (!tableNameNode.isPresent()) {
            return Optional.absent();
        }

        /**
         * 根据 tableName 节点构建 TableSegment {@link  #getTableSegment(ParserRuleContext)}
         */
        TableSegment result = getTableSegment(tableNameNode.get());

        //设置表的别名
        setAlias(tableNameNode.get(), result);
        return Optional.of(result);
    }
    
    private TableSegment getTableSegment(final ParserRuleContext tableNode) {

        // 从Context中获取Name节点
        ParserRuleContext nameNode = ExtractorUtils.getFirstChildNode(tableNode, RuleName.NAME);

        // 根据Name节点获取节点的起止位置以及节点内容
        TableSegment result = new TableSegment(nameNode.getStart().getStartIndex(), nameNode.getStop().getStopIndex(), nameNode.getText());

        // 从Context中获取表的Owner节点，如果有的话就设置Owner
        Optional<ParserRuleContext> ownerNode = ExtractorUtils.findFirstChildNodeNoneRecursive(tableNode, RuleName.OWNER);
        if (ownerNode.isPresent()) {
            result.setOwner(new SchemaSegment(ownerNode.get().getStart().getStartIndex(), ownerNode.get().getStop().getStopIndex(), ownerNode.get().getText()));
        }
        return result;
    }
    
    private void setAlias(final ParserRuleContext tableNameNode, final TableSegment tableSegment) {

        // 从 Context 中获取 Alias 节点，如果有的话就设置别名
        Optional<ParserRuleContext> aliasNode = ExtractorUtils.findFirstChildNode(tableNameNode.getParent(), RuleName.ALIAS);
        if (aliasNode.isPresent()) {
            tableSegment.setAlias(aliasNode.get().getText());
        }
    }
}
