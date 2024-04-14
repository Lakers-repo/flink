package org.apache.flink.table.examples.java.basics;

import java.time.Duration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.rules.physical.stream.IncrementalAggregateRule;
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy;

/**
 * @author haxi
 * @description: TODO
 * @date 2024/4/14 5:54 PM
 */
public class MiniBatchGroupAggExample {

    public static void main(String[] args) {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // set mini batch config
        tableEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                        AggregatePhaseStrategy.ONE_PHASE.name())
//                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
                .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                        Duration.ofSeconds(10))
                .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5L);
//                .set(IncrementalAggregateRule.TABLE_OPTIMIZER_INCREMENTAL_AGG_ENABLED(), true);

        // source ddl
        String srcTableDdl = "CREATE TABLE source_table (\n"
                + "    order_id STRING,\n"
                + "    user_id STRING,\n"
                + "    price BIGINT,\n"
                + "    rowtime TIMESTAMP(3),\n"
                + "    WATERMARK FOR rowtime AS rowtime\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '10',\n"
                + "  'fields.order_id.length' = '1',\n"
                + "  'fields.user_id.length' = '10',\n"
                + "  'fields.price.min' = '1',\n"
                + "  'fields.price.max' = '1000000'\n"
                + ")";

        // query
        String query = "select user_id, \n"
                + "count(order_id) as cnt \n"
                + "from source_table group by user_id";

        tableEnv.executeSql(srcTableDdl);
        System.out.println(tableEnv.explainSql(query, ExplainDetail.CHANGELOG_MODE));
    }
}
