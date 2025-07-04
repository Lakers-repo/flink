package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CanalJsonAndCDCDeduplicateExampleITCase {
    public static void main(String[] args) throws Exception {

        // set up execution environment
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.mini-batch.enabled", "false");
//        configuration.setString("table.exec.mini-batch.allow-latency", "10 s");
//        configuration.setString("table.exec.mini-batch.size", "2");
        configuration.setString("table.exec.resource.default-parallelism","1");
        configuration.setString("heartbeat.timeout", "360000000");
        configuration.setString("rest.connection-timeout", "360000000");
        configuration.setString("rest.idleness-timeout", "360000000");
        configuration.setString("heartbeat.interval", "360000000");
        configuration.setString("network.timeout", "100 min");
        configuration.setString("akka.ask.timeout", "360000000");
        configuration.setString("rpc.timeout", "360000000");
        configuration.setString("table.exec.source.cdc-events-duplicate","true");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        String ddl1Sql = "CREATE TABLE source_kafka (\n"
                + "  id varchar,\n"
                + "  order_no varchar,\n"
                + "  sku_title varchar\n"
                + " ,PRIMARY KEY (order_no) NOT ENFORCED"
                + ") WITH (\n"
                + "   'connector' = 'kafka',\n"
                + "   'properties.bootstrap.servers' = 't0-kafka.shizhuang-inc.net:18100',\n"
                + "   'properties.group.id' = 'asaa',\n"
//                + "   'scan.startup.mode' = 'latest-offset',\n"
                + "   'topic' = 'liuyang_test_1',"
//                + "    'value.format' = 'canal-json',"
                + "    'format' = 'canal-json'"
                + ")";
        tEnv.executeSql(ddl1Sql);

//        tEnv.executeSql("create view source_kafka_view as " +
//                "select * from (" +
//                "select * ,row_number() over(partition by order_no order by proctime() desc) as rn " +
//                "from source_kafka" +
//                ")a where a.rn=1");


        String ddl1Sql1 = "CREATE TABLE source_kafka_2 (\n"
                + "  id varchar,\n"
                + "  discount_no varchar,\n"
                + "  order_no varchar,\n"
                + "  discount_name varchar\n"
                + ") WITH (\n"
                + "   'connector' = 'kafka',\n"
                + "   'properties.bootstrap.servers' = 't0-kafka.shizhuang-inc.net:18100',\n"
                + "   'properties.group.id' = 'asdfsd',\n"
                + "   'scan.startup.mode' = 'latest-offset',\n"
                + "   'topic' = 'liuyang_test',"
                + "    'format' = 'json'"
                + ")";
        tEnv.executeSql(ddl1Sql1);

        tEnv.executeSql("create view source_kafka_2_view as " +
                "select * from (" +
                "select *,row_number() over(partition by order_no order by proctime() desc) as rn " +
                "from source_kafka_2" +
                ")a where a.rn=1");


        String ddlSqlSink = "CREATE TABLE MyUserTable (\n"
                + "  discount_no STRING,\n"
                + "  order_no STRING\n"
                + " ,PRIMARY KEY (order_no) NOT ENFORCED"
                + ") WITH (\n"
//                + "'connector' = 'print'\n"
                + "'connector' = 'upsert-kafka',\n"
                + "'topic' = 'liuyang_test',\n"
                + "'properties.bootstrap.servers' = 't0-kafka.shizhuang-inc.net:18100',\n"
                + "'value.format' = 'json',"
                + "'key.format' = 'json'"
                + ")";

        tEnv.executeSql(ddlSqlSink);

        System.out.println(tEnv.explainSql("insert into MyUserTable " +
                "select a.order_no," +
                "b.discount_no " +
                " from source_kafka a left join " +
                "source_kafka_2_view b on a.order_no=b.order_no", ExplainDetail.CHANGELOG_MODE));


//        Table table = tEnv.sqlQuery("select * from join_table");

//        tEnv.toRetractStream(table, Row.class).print();

//        System.out.println(env.getExecutionPlan());
//        env.execute("12");
    }
}
