package org.apache.flink.table.examples.java.basics;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author haxi
 * @description
 * @date 2024/4/11 14:45
 */
public class TemporalProcTimeJoinSQLExample {
    public static void main(String[] args) {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String transactions = "create table if not exists transactions (\n"
                + "                id string,\n"
                + "                currency_code string,\n"
                + "                total decimal(10,2),\n"
                + "                proctime as proctime()\n"
                + ") with (\n"
                + "                'connector' = 'datagen',\n"
                + "                'rows-per-second' = '100'\n"
                + ")";

        // 时态表（版本表）条件1：定义主键 latest version
        String dim = "create table if not exists currency_rates (\n"
                + "                currency_code string,\n"
                + "                eur_rate decimal(6,4),\n"
                + "                rate_time timestamp(3),\n"
                + "                primary key (currency_code) not enforced \n"
                + ")\n"
                + "        with (\n"
                + "                'connector' = 'datagen',\n"
                + "                'rows-per-second' = '100'\n"
                + ")";

        String query = "select\n"
                + "        t.id,\n"
                + "                t.total * c.eur_rate as total_eur,\n"
                + "        t.total,\n"
                + "                c.currency_code,\n"
                + "                t.proctime\n"
                + "        from\n"
                + "        transactions t\n"
                + "        left join\n"
                + "        currency_rates for system_time as of t.proctime as c\n"
                + "                on\n"
                + "        t.currency_code = c.currency_code";

        tableEnv.executeSql(transactions);
        tableEnv.executeSql(dim);
//        System.out.println(tableEnv.explainSql(query, ExplainDetail.CHANGELOG_MODE));
        System.out.println(tableEnv.explainSql(query, ExplainDetail.CHANGELOG_MODE));
//        tableEnv.executeSql("select * from transactions limit 10").print();
//        tableEnv.executeSql(query).print();
    }
}
