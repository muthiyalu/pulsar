/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.Clickhouse;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import java.util.Map;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * A Simple abstract class for Clickhouse sink
 * Users need to implement extractKeyValue function to use this sink
 */
public abstract class ClickhouseAbstractSink<K, V> implements Sink<byte[]> {




    Connection connection =  null;

    String createTableStmt=null;

    private ClickhouseSinkConfig clickhouseSinkConfig;




    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {


        clickhouseSinkConfig=ClickhouseSinkConfig.load(config);

        Class.forName(clickhouseSinkConfig.getClickJDBCDriverName());

        connection = DriverManager.getConnection(clickhouseSinkConfig.getJdbcUrl());
        Statement stmt = connection.createStatement(clickhouseSinkConfig.get);
        stmt.executeQuery(createTableStmt);


    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void write(Record<byte[]> record) {
        KeyValue<K, V> keyValue = extractKeyValue(record);
        PreparedStatement pstmt = connection.prepareStatement("INSERT INTO test_jdbc_example VALUES(?, ?, ?)");

    }


    public abstract KeyValue<K, V> extractKeyValue(Record<byte[]> record);
}