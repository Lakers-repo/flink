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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

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
public final class StreamSQLExample {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        final DataStream<Order> orderA =
//                env.fromCollection(
//                        Arrays.asList(
//                                new Order(1L, "beer", 3),
//                                new Order(1L, "diaper", 4),
//                                new Order(3L, "rubber", 2)));
//
//        final DataStream<Order> orderB =
//                env.fromCollection(
//                        Arrays.asList(
//                                new Order(2L, "pen", 3),
//                                new Order(2L, "rubber", 3),
//                                new Order(4L, "beer", 1)));
//
//        // convert the first DataStream to a Table object
//        // it will be used "inline" and is not registered in a catalog
//        final Table tableA = tableEnv.fromDataStream(orderA);
//
//        // convert the second DataStream and register it as a view
//        // it will be accessible under a name
//        tableEnv.createTemporaryView("TableB", orderB);
//
//        // union the two tables
//        final Table result =
//                tableEnv.sqlQuery(
//                        "SELECT * FROM "
//                                + tableA
//                                + " WHERE amount > 2 UNION ALL "
//                                + "SELECT * FROM TableB WHERE amount < 2");
//
//        // convert the Table back to an insert-only DataStream of type `Order`
//        tableEnv.toDataStream(result, Order.class).print();
//
//        // after the table program is converted to a DataStream program,
//        // we must use `env.execute()` to submit the job
//        env.execute();
        String sourceSql = "CREATE TABLE source_table (\n"
                + "    order_id STRING,\n"
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

        String sinkSql = "CREATE TABLE sink_table (\n"
                + "    order_id STRING,\n"
                + "    user_id STRING,\n"
                + "    row_num BIGINT\n"
//                + "    sum_result BIGINT,\n"
//                + "    avg_result DOUBLE,\n"
//                + "    min_result BIGINT,\n"
//                + "    max_result BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";



//        String middle = "create view mid_count as\n"
//                + "select order_id,\n"
//                + "       count(*) as count_result\n"
////                + "       sum(price) as sum_result,\n"
////                + "       avg(price) as avg_result,\n"
////                + "       min(price) as min_result,\n"
////                + "       max(price) as max_result\n"
//                + "from source_table\n"
//                + "group by order_id";

        String selectWhereSql = "insert into sink_table \n"
                + "select order_id, count(*) as count_result\n"
//                + "       sum(price) as sum_result,\n"
//                + "       avg(price) as avg_result,\n"
//                + "       min(price) as min_result,\n"
//                + "       max(price) as max_result\n"
                + "from (select order_id, user_id from (select order_id, user_id, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime DESC) as row_num from source_table) where row_num = 1"
                + ") group by order_id";

//        String query = "select order_id, user_id from (select order_id, user_id, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY user_id DESC) as row_num from source_table) where row_num >= 1 and row_num <=3";
//        String query = "insert into sink_table select order_id, cnt as count_result from (select order_id, count(*) as cnt from source_table group by order_id) where cnt <= 3";
//        String query = "select order_id, cnt as count_result from (select order_id, count(*) as cnt from source_table group by order_id) where cnt <=3";
//        String query = "select order_id, user_id, price, sum(price) OVER (PARTITION BY order_id ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as row_num from source_table";

//        String selectWhereSql = "insert into sink_table \n"
//                + "select order_id from source_table \n";
////                + "       sum(price) as sum_result,\n"
////                + "       avg(price) as avg_result,\n"
////                + "       min(price) as min_result,\n"
////                + "       max(price) as max_result\n"
////                + "where count_result > 1";
//        String query = "select user_id, count(*) from (select user_id, sum(price) from source_table group by user_id) tmp group by user_id";
        String query = "select tmp.user_id, sum(tmp.price1) from (select user_id, order_id, sum(price) as price1 from source_table group by user_id, order_id) tmp group by user_id";


        tableEnv.executeSql(sourceSql);
//        tableEnv.executeSql(sinkSql);
//        tableEnv.executeSql(middle);
//        tableEnv.executeSql(query);
        System.out.println(tableEnv.explainSql(query, ExplainDetail.CHANGELOG_MODE));
//        System.out.println(tableEnv.explainSql(query));
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
        public Order() {}

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
