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

package org.apache.shardingsphere.transaction.spi;

import org.apache.shardingsphere.spi.database.DatabaseType;
import org.apache.shardingsphere.transaction.core.ResourceDataSource;
import org.apache.shardingsphere.transaction.core.TransactionType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

/**
 * Sharding transaction manager.
 *
 * @author zhaojun
 *
 *   XA 分布式事务 {@link org.apache.shardingsphere.transaction.xa.XAShardingTransactionManager}
 *   Seata 分布式事务 {@link org.apache.shardingsphere.transaction.base.seata.at.SeataATShardingTransactionManager}
 */
public interface ShardingTransactionManager extends AutoCloseable {
    
    /**
     * Initialize sharding transaction manager.
     *
     * @param databaseType database type
     * @param resourceDataSources resource data sources
     *
     *  根据数据库类型和 ResourceDataSource 进行初始化
     */
    void init(DatabaseType databaseType, Collection<ResourceDataSource> resourceDataSources);
    
    /**
     * Get transaction type.
     *
     * @return transaction type
     *
     *  获取 TransactionType
     */
    TransactionType getTransactionType();
    
    /**
     * Judge is in transaction or not.
     * 
     * @return in transaction or not
     *
     *  判断是否在事务中
     */
    boolean isInTransaction();
    
    /**
     * Get transactional connection.
     *
     * @param dataSourceName data source name
     * @return connection
     * @throws SQLException SQL exception
     *
     * 获取支持事务的 Connection
     */
    Connection getConnection(String dataSourceName) throws SQLException;
    
    /**
     * Begin transaction.
     *
     *  开始事务
     */
    void begin();
    
    /**
     * Commit transaction.
     *
     *  提交事务
     */
    void commit();
    
    /**
     * Rollback transaction.
     *
     *  回滚事务
     */
    void rollback();
}
