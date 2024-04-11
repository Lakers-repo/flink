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

public final class StreamRegularJoinSQLExample {

    private static final Logger logger = LoggerFactory.getLogger(StreamRegularJoinSQLExample.class);
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

//        String sinkSql = "CREATE TABLE sink_table (\n"
//                + "    order_id STRING,\n"
//                + "    user_id STRING,\n"
//                + "    row_num BIGINT\n"
//                + ") WITH (\n"
//                + "  'connector' = 'print'\n"
//                + ")";
        String sinkSql = "CREATE TABLE sink_table (\n"
                + "    oid1 STRING,\n"
                + "    oid2 STRING,\n"
                + "    cnt1 BIGINT,\n"
                + "    cnt2 BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

//        String query = "select l.order_id, l.user_id, l.price"
//                + " from left_source_table l"
//                + " full outer join right_source_table r on l.order_id = r.order_id";
        String view1 = "create view view1 as select order_id, user_id, count(*) as cnt from left_source_table group by order_id, user_id";
        String view2 = "create view view2 as select order_id, user_id, count(*) as cnt from right_source_table group by order_id, user_id";
//        String view2 = "create view view2 as select order_id from right_source_table group by order_id";
//
//        String query = "select l.order_id, r.order_id, l.user_id, r.user_id, l.cnt, r.cnt from view1 l join view2 r on l.order_id = r.order_id";
        String query = "select cnt from view1 where order_id in (select order_id from view2)";
//        String query = "select order_id, user_id, price from left_source_table where order_id not in (select order_id from right_source_table)";

//        String query = "select l.order_id, l.user_id, l.price, r.order_id, l.proctime, r.proctime"
//                + " from left_source_table l, right_source_table r";

        tableEnv.executeSql(left);
        tableEnv.executeSql(right);
        tableEnv.executeSql(view1);
        tableEnv.executeSql(view2);
//        tableEnv.executeSql(sinkSql);
        logger.info(tableEnv.explainSql(query, ExplainDetail.CHANGELOG_MODE));
    }
}

