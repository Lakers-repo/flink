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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.examples.java.functions.BitMapCountWithRetract;
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
public class StreamSQLAggregateExampleForSourceCode {
    private static final Logger logger = LoggerFactory.getLogger(StreamSQLAggregateExampleForSourceCode.class);

    public static void main(String[] args) throws Exception {
        // set up execution environment
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.mini-batch.enabled", "false");
//        configuration.setString("table.exec.mini-batch.allow-latency", "10 s");
//        configuration.setString("table.exec.mini-batch.size", "2");
        configuration.setString("table.exec.resource.default-parallelism","1");
        configuration.setString("heartbeat.timeout", "360000000");
        configuration.setString("rest.connection-timeout", "360000000");
        configuration.setString("rest.idleness-timeout", "360000000");
        configuration.setString("heartbeat.interval", "360000000");
        configuration.setString("network.timeout", "100 min");
        configuration.setString("akka.ask.timeout", "360000000");
        configuration.setString("rpc.timeout", "360000000");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // write source data into temporary file and get the absolute path
        String contents =
                "0,25041737386170,10,2025-04-17 17:27:48\n"
                        + "1,25041737386170,20,2025-04-17 17:28:58";
//                        + "0,25041737386170,20,2025-04-17 17:29:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:29:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:30:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:30:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:31:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:31:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:32:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:32:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:33:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:33:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:34:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:35:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:36:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:37:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:38:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:38:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:39:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:40:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:41:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:42:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:43:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:44:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:45:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:46:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:47:58\n"
//                        + "0,25041737386170,20,2025-04-17 17:48:58\n"
//                        + "0,25041737386170,50,2025-04-17 17:49:58";
        String path = createTempFile(contents);

        // register table via DDL with watermark,
        // the events are out of order, hence, we use 3 seconds to wait the late events
        String ddl =
                "CREATE TABLE ticket (\n"
                        + "  group_id BIGINT,\n"
                        + "  user_id BIGINT,\n"
                        + "  status INT,\n"
                        + "  create_time VARChAR\n"
//                        + "  proc_time as PROCTIME()\n"
                        + ") WITH (\n"
                        + "  'connector.type' = 'filesystem',\n"
                        + "  'connector.path' = '"
                        + path
                        + "',\n"
                        + "  'format.type' = 'csv'\n"
                        + ")";
        tEnv.executeSql(ddl);

//        tEnv.createTemporarySystemFunction("BitMapCountWithRetract", BitMapCountWithRetract.class);

        String dedupSql = "create view dedup as select * from (select *, row_number() over (partition by user_id order by create_time desc) as rn from ticket) tmp where tmp.rn = 1";

        tEnv.executeSql(dedupSql);

//        String unionSql = "create view union_view as select *,cast('node' as varchar) as node, cast('A' as varchar) as tree from dedup union all select *,cast('node' as varchar) as node, cast('B' as varchar) as tree from dedup";

//        tEnv.executeSql(unionSql);

        String aggSql = "create view agg as select group_id,count(*) as cnt from dedup group by group_id";

        String sinkDdl = "CREATE TABLE sink_table (\n"
                + "    group_id BIGINT,\n"
//                + "    group_id BIGINT,\n"
                + "    cnt BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

        tEnv.executeSql(aggSql);

        tEnv.executeSql(sinkDdl);

        tEnv.executeSql("insert into sink_table select * from agg");
//        logger.info(tEnv.explainSql("insert into sink_table select * from agg", ExplainDetail.CHANGELOG_MODE));
    }


    /** Creates a temporary file with the contents and returns the absolute path. */
    private static String createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("ticket", ".csv");
        tempFile.deleteOnExit();
        FileUtils.writeFileUtf8(tempFile, contents);
        return tempFile.toURI().toString();
    }
}
