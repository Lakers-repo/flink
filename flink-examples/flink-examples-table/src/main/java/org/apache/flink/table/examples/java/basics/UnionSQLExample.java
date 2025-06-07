package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UnionSQLExample {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // set up the Java DataStream API
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.setMaxParallelism(1);

        String sourceSql1 = "CREATE TABLE source_table1 (\n"
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

        String sourceSql2 = "CREATE TABLE source_table2 (\n"
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

        String union = "create view source_union as "
                + "select order_id, user_id, price, proctime from source_table1 "
                + "union "
                + "select order_id, user_id, price, proctime from source_table2";

        String sinkTable = "CREATE TABLE sink_table (\n"
                + "    order_id STRING,\n"
                + "    amount BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

//        String sinkSql = "insert into sink_table select order_id, sum(price) as amount from source_union group by order_id";
        String sinkSql = "insert into sink_table select order_id, price as amount from source_union";

        tableEnv.executeSql(sourceSql1);
        tableEnv.executeSql(sourceSql2);
        tableEnv.executeSql(union);
        tableEnv.executeSql(sinkTable);
        System.out.println(tableEnv.explainSql(sinkSql, ExplainDetail.CHANGELOG_MODE));
    }
}
