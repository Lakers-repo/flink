package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;

public class SimpleFlinkIssue26051 {
    public static void main(String[] args) throws Exception {

        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        Configuration configuration = tableEnv.getConfig().getConfiguration();
//        configuration.set(OptimizerConfigOptions.TABLE_EXEC_MERGE_CALC, true);
        String sourceSql = "CREATE TABLE source_table (\n"
                + "    id STRING,\n"
                + "    name STRING,\n"
                + "    age STRING,\n"
                + "    money STRING\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.id.length' = '10',\n"
                + "  'fields.name.length' = '10',\n"
                + "  'fields.age.length' = '10',\n"
                + "  'fields.money.length' = '10'\n"
                + ")";

        String sinkSql = "CREATE TABLE sink_table (\n"
                + "    id STRING,\n"
                + "    s BIGINT\n"
                + ") WITH (\n"
                + "  'connector' = 'print'\n"
                + ")";

        String row_number_sql =
                "create view where_view as "
                        + "select * from "
                        + "(select *,"
                        + " ROW_NUMBER() OVER (PARTITION BY id ORDER BY name DESC) as row_num from source_table) where row_num = 1";

//        String case_when_where_sql = "create view case_when_where_view as select * from where_view where age in (1,6,10,15)";
        String case_when_where_sql = "select id,case when age < 9 then 1 else -1 end as age from where_view where age in (1,6,10,15)";

//        String query =
//                "insert into sink_table select id, sum(s) as s from case_when_where_view group by id";
//        tableEnv.executeSql("set table.exec.merge.calc = 'true'");
        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql(row_number_sql);
        tableEnv.explainSql(case_when_where_sql);
//        tableEnv.executeSql(query);
//        System.out.println(tableEnv.explainSql(query, ExplainDetail.CHANGELOG_MODE));
    }
}
