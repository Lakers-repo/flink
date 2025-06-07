package org.apache.flink.table.examples.java.basics;

import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkIssue26051 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        DataStream<Tuple12<String, Integer, Integer, String, Integer, Integer, Integer, Integer,
                Integer, String, String, String>> oriStream = env.addSource(new CustomSourceRowNumber());
        Table testTable = tableEnv.fromDataStream(
                oriStream,
                $("biz_bill_no"),
                $("task_type"),
                $("task_mode"),
                $("parent_task_no"),
                $("total_stage_num"),
                $("current_stage_index"),
                $("use_pre_task_owner"),
                $("poi_type"),
                $("biz_origin_bill_type"),
                $("sowing_task_no"),
                $("dt"),
                $("sowing_task_detail_id"));
        tableEnv.createTemporaryView("wosOutSowingTaskDetail", testTable);

        tableEnv.executeSql(
                "create view temp1 as SELECT `biz_bill_no`" +
                        ",task_type" +
                        ",task_mode" +
                        ",parent_task_no" +
                        ",total_stage_num" +
                        ",current_stage_index" +
                        ",use_pre_task_owner" +
                        ",poi_type" +
                        ",biz_origin_bill_type" +
                        ",sowing_task_no" +
                        " FROM (" +
                        "    SELECT *," +
                        "        ROW_NUMBER() OVER (PARTITION BY sowing_task_detail_id ORDER BY task_type desc) AS rn"
                        +
                        "     FROM wosOutSowingTaskDetail" +
                        "    ) tmp" +
                        " WHERE rn = 1");

//        tableEnv.executeSql("create view temp2 as SELECT `biz_bill_no`"
//                + ",parent_task_no"
//                + ",poi_type"
//                + ",biz_origin_bill_type"
//                + ",sowing_task_no,"
//                + "task_type, task_mode, "
//                + "total_stage_num, "
//                + "current_stage_index,"
//                + "use_pre_task_owner" +
//                "FROM temp1");

        System.out.println(tableEnv.explainSql("select temp1.*, "
                + "case when task_mode = 51 THEN parent_task_no "
                + "when task_mode = 40 AND total_stage_num >= 2 AND current_stage_index >= 2 AND use_pre_task_owner = 1 THEN parent_task_no "
                + "ELSE sowing_task_no END AS parent_task_no_cw "
                + "from temp1 "
                + "where task_type = 21 "
                + "and task_mode IN (51, 40) "
                + "and poi_type = 2 "
                + "and biz_origin_bill_type not in (111,112,113,114)"));
    }
}

class CustomSourceRowNumber implements SourceFunction<Tuple12<String, Integer, Integer, String, Integer, Integer,
        Integer, Integer,
        Integer, String, String, String>> {
    private boolean isRuning = true;

    @Override
    public void run(
            SourceContext<Tuple12<String, Integer, Integer, String, Integer, Integer, Integer, Integer,
                    Integer, String, String, String>> sourceContext) throws Exception {
        while (isRuning) {
            sourceContext.collect(Tuple12.of(
                    "xxx",
                    21,
                    51,
                    "yyy",
                    1,
                    1,
                    0,
                    2,
                    110,
                    "zzz",
                    "aaa",
                    "bbb"));
            sourceContext.collect(Tuple12.of(
                    "xxx",
                    21,
                    40,
                    "yyy",
                    2,
                    2,
                    1,
                    2,
                    110,
                    "zzz",
                    "aaa",
                    "bbb"));
            Thread.sleep(Integer.MAX_VALUE);
        }
    }

    @Override
    public void cancel() {
        isRuning = false;
    }
}
