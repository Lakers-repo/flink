package org.apache.flink.table.examples.java.basics;

/**
 * @author haxi
 * @description
 * @date 2024/4/5 16:47
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamIntervalJoinSQLExample {

    private static final Logger logger = LoggerFactory.getLogger(StreamIntervalJoinSQLExample.class);
    public static void main(String[] args) throws Exception {

        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String left = "CREATE TABLE left_source_table (\n"
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

        String right = "CREATE TABLE right_source_table (\n"
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

        String query = "select l.order_id, l.user_id, r.order_id, r.user_id, l.price, r.price "
                + "from left_source_table l, right_source_table r "
                + "where l.order_id = r.order_id "
                + "and l.proctime BETWEEN r.proctime - INTERVAL '10' SECOND AND r.proctime + INTERVAL '5' SECOND";


        tableEnv.executeSql(left);
        tableEnv.executeSql(right);
        logger.info(tableEnv.explainSql(query, ExplainDetail.CHANGELOG_MODE));
    }
}

