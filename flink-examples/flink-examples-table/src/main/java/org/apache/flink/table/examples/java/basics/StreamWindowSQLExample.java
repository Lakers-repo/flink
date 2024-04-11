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

import org.apache.flink.connector.file.src.impl.ContinuousFileSplitEnumerator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Simple example for demonstrating the use of SQL in Java.
 *
 * <p>Usage: {@code ./bin/flink run ./examples/table/StreamWindowSQLExample.jar}
 *
 * <p>This example shows how to: - Register a table via DDL - Declare an event time attribute in the
 * DDL - Run a streaming window aggregate on the registered table
 */
public class StreamWindowSQLExample {
    private static final Logger logger = LoggerFactory.getLogger(StreamWindowSQLExample.class);

    public static void main(String[] args) throws Exception {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // write source data into temporary file and get the absolute path
        String contents =
                "1,beer,3,2019-12-12 00:00:01\n"
                        + "1,diaper,4,2019-12-12 00:00:02\n"
                        + "2,pen,3,2019-12-12 00:00:04\n"
                        + "2,rubber,3,2019-12-12 00:00:06\n"
//                        + "3,rubber,2,2019-12-12 00:00:05\n"
                        + "4,beer,1,2019-12-12 00:00:08";
        String path = createTempFile(contents);

        // write source data into temporary file and get the absolute path
        String contents1 =
                "1,beer,3,2019-12-12 00:00:06\n"
                        + "1,diaper,4,2019-12-12 00:00:07\n"
                        + "2,pen,3,2019-12-12 00:00:10\n"
                        + "2,rubber,3,2019-12-12 00:00:14\n"
                        + "3,rubber,2,2019-12-12 00:00:16\n"
                        + "4,beer,1,2019-12-12 00:00:18";
        String path1 = createTempFile(contents1);


        // register table via DDL with watermark,
        // the events are out of order, hence, we use 3 seconds to wait the late events
        String ddl =
                "CREATE TABLE orders (\n"
                        + "  user_id INT,\n"
                        + "  product STRING,\n"
                        + "  amount INT,\n"
                        + "  ts TIMESTAMP(3),\n"
                        + "  WATERMARK FOR ts AS ts\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(ddl);

        String ddl1 =
                "CREATE TABLE payments (\n"
                        + "  user_id INT,\n"
                        + "  product STRING,\n"
                        + "  amount INT,\n"
                        + "  ts TIMESTAMP(3),\n"
                        + "  WATERMARK FOR ts AS ts\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path1
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(ddl1);

        // run a SQL query on the table and retrieve the result as a new Table
//        String query_group_window_agg =
//                "SELECT\n"
//                        + "  CAST(TUMBLE_START(ts, INTERVAL '5' SECOND) AS STRING) window_start,\n"
//                        + "  COUNT(*) order_num\n"
////                        + "  SUM(amount) total_amount,\n"
////                        + "  COUNT(DISTINCT product) unique_products\n"
//                        + "FROM orders\n"
//                        + "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

        /*
        SELECT *
  FROM (
    SELECT bidtime, price, item, supplier_id, window_start, window_end, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as rownum
    FROM TABLE(
               TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
  ) WHERE rownum <= 3;
         */
//        String query_window_tvf_agg =
//                "SELECT\n" + "window_start, window_end,\n"
//                        + "  COUNT(*) order_num\n"
//                        + "FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '5' SECONDS))\n"
//                        + "GROUP BY window_start, window_end";

//        String query_window_topn =
//                "select user_id,product,amount, window_start,window_end "
//                        + "from ("
//                        + "SELECT user_id, product, amount, window_start, window_end, ROW_NUMBER() OVER (PARTITION BY user_id, window_start, window_end ORDER BY amount DESC) as row_num\n"
//                        + "FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '5' SECONDS))\n"
//                        + ") where row_num <= 3";

//        String query =
//                "select user_id,product,amount, window_start,window_end "
//                        + "from ("
//                        + "SELECT user_id, product, amount, window_start, window_end, ROW_NUMBER() OVER (PARTITION BY user_id, window_start, window_end ORDER BY ts DESC) as row_num\n"
//                        + "FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '5' SECONDS))\n"
//                        + ") where row_num <= 1";
//            String query = "select user_id, product, amount, SUM(amount) OVER (PARTITION BY user_id ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as row_num from orders";
            String query = "select l.user_id, r.user_id from orders l, payments r where l.user_id=r.user_id and l.ts BETWEEN r.ts - INTERVAL '10' SECOND AND r.ts + INTERVAL '5' SECOND";


//        logger.info(tEnv.explainSql(query, ExplainDetail.CHANGELOG_MODE));
        tEnv.executeSql(query).print();
  // should output:
  // +----+--------------------------------+--------------+--------------+-----------------+
  // | op |                   window_start |    order_num | total_amount | unique_products |
  // +----+--------------------------------+--------------+--------------+-----------------+
  // | +I |        2019-12-12 00:00:00.000 |            3 |           10 |               3 |
  // | +I |        2019-12-12 00:00:05.000 |            3 |            6 |               2 |
  // +----+--------------------------------+--------------+--------------+-----------------+

}

/** Creates a temporary file with the contents and returns the absolute path. */
private static String createTempFile(String contents) throws IOException {
  File tempFile = File.createTempFile("orders", ".csv");
  tempFile.deleteOnExit();
  FileUtils.writeFileUtf8(tempFile, contents);
  return tempFile.toURI().toString();
}
}
