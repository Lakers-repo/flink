/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Simple example for demonstrating the use of SQL on a table backed by a {@link DataStream} in Java
 * DataStream API.
 *
 * <p>In particular, the example shows how to
 *
 * <ul>
 *   <li>convert two bounded data streams to tables,
 *   <li>register a table as a view under a name,
 *   <li>run a stream SQL query on registered and unregistered tables,
 *   <li>and convert the table back to a data stream.
 * </ul>
 *
 * <p>The example executes a single Flink job. The results are written to stdout.
 */
public final class StreamSQLAggregateExample {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.setMaxParallelism(1);

        String sourceSql = "CREATE TABLE source_table (\n"
                + "    group_id STRING,\n"
                + "    user_id STRING,\n"
                + "    price BIGINT,\n"
                + "    proctime AS PROCTIME()\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.order_id.length' = '1',\n"
                + "  'fields.user_id.length' = '10',\n"
                + "  'fields.price.min' = '1',\n"
                + "  'fields.price.max' = '1000000'\n"
                + ")";

        String sinkSql = "CREATE TABLE sink_table1 (\n"
                + "    order_id STRING,\n"
                + "    amount BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

        String dmlQuery = "insert into sink_table \n"
                + "select order_id, count(*) as count_result\n"
                + "from (select order_id, user_id from (select order_id, user_id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY proctime DESC) as row_num from source_table) where row_num = 1"
                + ") group by order_id";

        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql(dmlQuery);
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /** Simple POJO. */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        // for POJO detection in DataStream API
        public Order() {
        }

        // for structured type detection in Table API
        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{"
                    + "user="
                    + user
                    + ", product='"
                    + product
                    + '\''
                    + ", amount="
                    + amount
                    + '}';
        }
    }
}
