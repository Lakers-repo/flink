package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamLookupJoinExample {
    private static final Logger logger = LoggerFactory.getLogger(StreamLookupJoinExample.class);

    public static void main(String[] args) {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create the source table
        String source =
                "CREATE TABLE src (\n"
                        + "                `id` BIGINT,\n"
                        + "                `len` INT,\n"
                        + "                `content` STRING,\n"
                        + "                `proctime` AS PROCTIME()\n"
                        + "        ) WITH (\n"
                        + "                'connector' = 'values',\n"
                        + "                'data-id' = '4'\n"
                        + "        )";

        // create the lookup table
        String lookup =
                "CREATE TABLE user_table (\n"
                        + "                `age` INT,\n"
                        + "                `id` BIGINT,\n"
                        + "                `ts` timestamp(3),\n"
                        + "                `name` STRING\n"
                        + "        ) WITH (\n"
                        + "\n"
                        + "                'lookup.cache' = 'FULL',\n"
                        + "                'lookup.full-cache.periodic-reload.interval' = '10s',\n"
                        //                + "                'lookup.partial-cache.max-rows' =
                        // '1000',\n"
                        + "                'connector' = 'values',\n"
                        + "                'data-id' = '5'\n"
                        + "        )";

        // create the sink table
        String sink =
                "CREATE TABLE sink (\n"
                        + "                `id` BIGINT,\n"
                        + "                `len` INT,\n"
                        + "                `content` STRING,\n"
                        + "                `name` STRING\n"
                        + "        ) WITH (\n"
                        + "                'connector' = 'print'\n"
                        + "        )";

//        String query =
//                "SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table "
//                        + "for system_time as of T.proctime AS D ON T.id = D.id";

        String query_sink =
                "insert into sink SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table /*+ PARTITIONED_JOIN */"
                        + "for system_time as of T.proctime AS D ON T.id = D.id";

//        String query_sink =
//                "insert into sink SELECT T.id, T.len, T.content, D.name FROM src AS T JOIN user_table"
//                        + " for system_time as of T.proctime AS D ON T.id = D.id "
//                        + "where D.ts <= PROCTIME()";

        tableEnv.executeSql(source);
        tableEnv.executeSql(lookup);
        tableEnv.executeSql(sink);
        //        tableEnv.executeSql(query_sink);

        //        tableEnv.executeSql(query);
        //        logger.info(tableEnv.explainSql(query));
        //        System.out.println(tableEnv.explainSql(query_sink,
        // ExplainDetail.JSON_EXECUTION_PLAN));
        System.out.println(tableEnv.explainSql(query_sink, ExplainDetail.CHANGELOG_MODE));
    }
}
