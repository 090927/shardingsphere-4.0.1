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

package org.apache.shardingsphere.shardingjdbc.executor;

import lombok.Getter;
import org.apache.shardingsphere.core.constant.ConnectionMode;
import org.apache.shardingsphere.core.execute.engine.ShardingExecuteGroup;
import org.apache.shardingsphere.core.execute.sql.StatementExecuteUnit;
import org.apache.shardingsphere.core.execute.sql.execute.SQLExecuteCallback;
import org.apache.shardingsphere.core.execute.sql.execute.result.MemoryQueryResult;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.statement.MasterSlavePreparedStatement;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.statement.ShardingPreparedStatement;
import org.apache.shardingsphere.underlying.execute.QueryResult;
import org.apache.shardingsphere.core.execute.sql.execute.result.StreamQueryResult;
import org.apache.shardingsphere.core.execute.sql.execute.threadlocal.ExecutorExceptionHandler;
import org.apache.shardingsphere.core.execute.sql.prepare.SQLExecutePrepareCallback;
import org.apache.shardingsphere.core.route.RouteUnit;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.connection.ShardingConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

/**
 * Prepared statement executor.
 * 
 * @author zhangliang
 * @author caohao
 * @author maxiaoguang
 * @author panjuan
 */
public final class PreparedStatementExecutor extends AbstractStatementExecutor {
    
    @Getter
    private final boolean returnGeneratedKeys;
    
    public PreparedStatementExecutor(
            final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability, final boolean returnGeneratedKeys, final ShardingConnection shardingConnection) {
        super(resultSetType, resultSetConcurrency, resultSetHoldability, shardingConnection);
        this.returnGeneratedKeys = returnGeneratedKeys;
    }
    
    /**
     * Initialize executor.
     *
     * @param routeResult route result
     * @throws SQLException SQL exception
     */
    public void init(final SQLRouteResult routeResult) throws SQLException {
        setSqlStatementContext(routeResult.getSqlStatementContext());
        getExecuteGroups().addAll(obtainExecuteGroups(routeResult.getRouteUnits()));
        cacheStatements();
    }
    
    private Collection<ShardingExecuteGroup<StatementExecuteUnit>> obtainExecuteGroups(final Collection<RouteUnit> routeUnits) throws SQLException {
        return getSqlExecutePrepareTemplate().getExecuteUnitGroups(routeUnits, new SQLExecutePrepareCallback() {
            
            @Override
            public List<Connection> getConnections(final ConnectionMode connectionMode, final String dataSourceName, final int connectionSize) throws SQLException {
                return PreparedStatementExecutor.super.getConnection().getConnections(connectionMode, dataSourceName, connectionSize);
            }
            
            @Override
            public StatementExecuteUnit createStatementExecuteUnit(final Connection connection, final RouteUnit routeUnit, final ConnectionMode connectionMode) throws SQLException {
                return new StatementExecuteUnit(routeUnit, createPreparedStatement(connection, routeUnit.getSqlUnit().getSql()), connectionMode);
            }
        });
    }
    
    @SuppressWarnings("MagicConstant")
    private PreparedStatement createPreparedStatement(final Connection connection, final String sql) throws SQLException {
        return returnGeneratedKeys ? connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
                : connection.prepareStatement(sql, getResultSetType(), getResultSetConcurrency(), getResultSetHoldability());
    }
    
    /**
     * Execute query.
     *
     * @return result set list
     * @throws SQLException SQL exception
     */
    public List<QueryResult> executeQuery() throws SQLException {
        final boolean isExceptionThrown = ExecutorExceptionHandler.isExceptionThrown();

        // 创建 SQLExecuteCallback 并执行查询
        SQLExecuteCallback<QueryResult> executeCallback = new SQLExecuteCallback<QueryResult>(getDatabaseType(), isExceptionThrown) {
            
            @Override
            protected QueryResult executeSQL(final String sql, final Statement statement, final ConnectionMode connectionMode) throws SQLException {

                /**
                 *  [getQueryResult] {@link PreparedStatementExecutor#getQueryResult(Statement, ConnectionMode)}
                 */
                return getQueryResult(statement, connectionMode);
            }
        };

        /**
         *  执行 SQLExecuteCallback 并返回结果 {@link AbstractStatementExecutor#executeCallback(SQLExecuteCallback)}
         */
        return executeCallback(executeCallback);
    }
    
    private QueryResult getQueryResult(final Statement statement, final ConnectionMode connectionMode) throws SQLException {
        PreparedStatement preparedStatement = (PreparedStatement) statement;

        /**
         * 通过 Statement 执行 SQL 并获取结果
         *
         *   1、分片结构,执行查询 {@link ShardingPreparedStatement#executeQuery()}
         *   2、主从结构,执行查询 {@link MasterSlavePreparedStatement#executeQuery()}
         */
        ResultSet resultSet = preparedStatement.executeQuery();
        getResultSets().add(resultSet);

        // 根据连接模式来确认构建结果
        return ConnectionMode.MEMORY_STRICTLY == connectionMode ? new StreamQueryResult(resultSet) : new MemoryQueryResult(resultSet);
    }
    
    /**
     * Execute update.
     * 
     * @return effected records count
     * @throws SQLException SQL exception
     */
    public int executeUpdate() throws SQLException {
        final boolean isExceptionThrown = ExecutorExceptionHandler.isExceptionThrown();
        SQLExecuteCallback<Integer> executeCallback = SQLExecuteCallbackFactory.getPreparedUpdateSQLExecuteCallback(getDatabaseType(), isExceptionThrown);
        List<Integer> results = executeCallback(executeCallback);
        if (isAccumulate()) {
            return accumulate(results);
        } else {
            return results.get(0);
        }
    }
    
    private int accumulate(final List<Integer> results) {
        int result = 0;
        for (Integer each : results) {
            result += null == each ? 0 : each;
        }
        return result;
    }
    
    /**
     * Execute SQL.
     *
     * @return return true if is DQL, false if is DML
     * @throws SQLException SQL exception
     */
    public boolean execute() throws SQLException {
        boolean isExceptionThrown = ExecutorExceptionHandler.isExceptionThrown();
        SQLExecuteCallback<Boolean> executeCallback = SQLExecuteCallbackFactory.getPreparedSQLExecuteCallback(getDatabaseType(), isExceptionThrown);
        List<Boolean> result = executeCallback(executeCallback);
        if (null == result || result.isEmpty() || null == result.get(0)) {
            return false;
        }
        return result.get(0);
    }
}
