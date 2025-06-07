package org.apache.flink.table.examples.java.basics;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkIssueDebugExample {
    public static void main(String[] args) throws Exception {

        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
//                + "    user_id STRING,\n"
                + "    cnt BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

//        String rowNumber
//                = "create view row_number_view as "
//                + "select order_id from (select order_id, "
//                + "ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime DESC) as row_num from source_table) where row_num = 1";

        String where =
                "create view where_view as "
                        + "select case when order_id = '1' then order_id else '0' end as order_id from "
                        + "(select order_id from (select order_id,"
                        + " ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime DESC) as row_num from source_table) where row_num = 1)"
                        + "where order_id is not null";

        String query =
                "insert into sink_table select order_id, count(*) as cnt from where_view group by order_id";
        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);
//        tableEnv.executeSql(rowNumber);
        tableEnv.executeSql(where);
        tableEnv.executeSql(query);
//        System.out.println(tableEnv.explainSql(query, ExplainDetail.CHANGELOG_MODE));
    }
}
