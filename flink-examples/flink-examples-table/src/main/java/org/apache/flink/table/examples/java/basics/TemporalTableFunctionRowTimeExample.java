package org.apache.flink.table.examples.java.basics;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author haxi
 * @description
 * @date 2024/4/11 15:03
 */
public class TemporalTableFunctionRowTimeExample {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 事实表: transactions
        tableEnv
                .executeSql(
                        "create temporary table transactions (\n" +
                                "    id string,\n" +
                                "    currency_code string,\n" +
                                "    total decimal(10,2),\n" +
                                "    transaction_time timestamp(3), \n" +
                                "    watermark for `transaction_time` as transaction_time - interval '15' second \n"
                                +
                                ") with (\n" +
                                "    'connector' = 'datagen',\n" +
                                "    'rows-per-second' = '100'\n" +
                                ")")
                .print();

        // 维表: currency_rates
        tableEnv
                .executeSql(
                        "create temporary table currency_rates (\n" +
                                "    currency_code string,\n" +
                                "    eur_rate decimal(6,4),\n" +
                                "    rate_time timestamp(3),\n" +
//                                "    primary key (currency_code) not enforced,\n"
//                                +
                                "    watermark for `rate_time` as rate_time - interval '15' second \n"
                                +
                                ")\n" +
                                "with (\n" +
                                "    'connector' = 'datagen',\n" +
                                "    'rows-per-second' = '100'\n" +
                                ");")
                .print();

        // 注册 Temporal Table Function
        TemporalTableFunction rates = tableEnv
                .from("currency_rates")
                .createTemporalTableFunction($("rate_time"), $("currency_code"));

        tableEnv.createTemporarySystemFunction("rates", rates);

        String query = "select\n" +
                "    t.id,\n" +
                "    t.total * c.eur_rate as total_eur,\n" +
                "    t.total,\n" +
                "    c.currency_code,\n" +
                "    t.transaction_time\n" +
                "from\n" +
                "    transactions t,\n" +
                "    lateral table (rates(transaction_time)) as c\n" +
                "where\n" +
                "    t.currency_code = c.currency_code";
        System.out.println(tableEnv.explainSql(query));

        // 使用 Temporal Table Function + LATERAL TABLE 关键字实现 Temporal Join
        // 在时态维度上关联时指定的是事实表的事件时间（transaction_time）
//        tableEnv.executeSql(query).print();

    }
}
