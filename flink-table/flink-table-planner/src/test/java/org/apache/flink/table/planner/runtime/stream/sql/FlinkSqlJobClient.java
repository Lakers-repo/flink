package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlinkSqlJobClient {
    public static void main(String[] args) throws Exception {
        String sql = "\n"
                + "CREATE TABLE user_behavior (\n"
                + "  `user_id`       BIGINT,\n"
                + "  `user_code`     STRING,\n"
                + "  `user_score`    FLOAT,\n"
                + "  `event_time`    TIMESTAMP(3)\n"
                + ") WITH (\n"
                + " 'connector' = 'kafka',\n"
                + " 'topic' = 'csv-topic',\n"
                + " 'properties.bootstrap.servers' = 'flink-test-01:9092,flink-test-02:9092,flink-test-03:9092',\n"
                + " 'properties.group.id' = 'testGroup-flink',\n"
                + " 'scan.startup.mode' = 'earliest-offset',\n"
                + " 'format' = 'csv',\n"
                + " 'csv.ignore-parse-errors' = 'true',\n"
                + " 'csv.allow-comments' = 'true'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE console_sink (\n"
                + "  `user_id`       BIGINT,\n"
                + "  `user_code`     STRING,\n"
                + "  `user_score`    FLOAT,\n"
                + "  `event_time`    TIMESTAMP(3)\n"
                + ") WITH (\n"
                + " 'connector' = 'kafka',\n"
                + " 'topic' = 'csv-topic',\n"
                + " 'properties.bootstrap.servers' = 'flink-test-01:9092,flink-test-02:9092,flink-test-03:9092',\n"
                + " 'format' = 'csv',\n"
                + " 'sink.parallelism' = '3'\n"
                + ");\n"
                + "\n"
                + "CREATE VIEW result_view AS\n"
                + "select\n"
                + "  `user_id`      ,\n"
                + "  `user_code`    ,\n"
                + "  `user_score`   ,\n"
                + "  `event_time`\n"
                + "from user_behavior;\n"
                + "\n"
                + "\n"
                + "insert into console_sink\n"
                + "select\n"
                + "  `user_id`      ,\n"
                + "  `user_code`    ,\n"
                + "  `user_score`   ,\n"
                + "  `event_time`\n"
                + "from result_view;\n"
                + "\n"
                + "CREATE TABLE user_behavior1 (\n"
                + "  `user_id`       BIGINT,\n"
                + "  `user_code`     STRING,\n"
                + "  `user_score`    FLOAT,\n"
                + "  `event_time`    TIMESTAMP(3)\n"
                + ") WITH (\n"
                + " 'connector' = 'kafka',\n"
                + " 'topic' = 'csv-topic1',\n"
                + " 'properties.bootstrap.servers' = 'flink-test-01:9092,flink-test-02:9092,flink-test-03:9092',\n"
                + " 'properties.group.id' = 'testGroup-flink',\n"
                + " 'scan.startup.mode' = 'earliest-offset',\n"
                + " 'format' = 'csv',\n"
                + " 'csv.ignore-parse-errors' = 'true',\n"
                + " 'csv.allow-comments' = 'true'\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE console_sink1 (\n"
                + "  `user_id`       BIGINT,\n"
                + "  `user_code`     STRING,\n"
                + "  `user_score`    FLOAT,\n"
                + "  `event_time`    TIMESTAMP(3)\n"
                + ") WITH (\n"
                + " 'connector' = 'kafka',\n"
                + " 'topic' = 'csv-topic1',\n"
                + " 'properties.bootstrap.servers' = 'flink-test-01:9092,flink-test-02:9092,flink-test-03:9092',\n"
                + " 'format' = 'csv',\n"
                + " 'sink.parallelism' = '3'\n"
                + ");\n"
                + "\n"
                + "CREATE VIEW result_view1 AS\n"
                + "select\n"
                + "  `user_id`      ,\n"
                + "  `user_code`    ,\n"
                + "  `user_score`   ,\n"
                + "  `event_time`\n"
                + "from user_behavior1;\n"
                + "\n"
                + "\n"
                + "insert into console_sink1\n"
                + "select\n"
                + "  `user_id`      ,\n"
                + "  `user_code`    ,\n"
                + "  `user_score`   ,\n"
                + "  `event_time`\n"
                + "from result_view1;\n";
        List<SqlNode> sqlNodes = splitSqlContext(sql, SqlDialect.DEFAULT);
        sqlNodes.forEach(node -> System.out.println(node.toString() + "\n======================================"));
        sqlNodes.forEach(node -> System.out.println(node.getKind() + "\n======================================"));
    }

    public static List<SqlNode> splitSqlContext(String sqlContent, SqlDialect sqlDialect)
            throws SqlParseException {
        SqlParser.Config parserConfig = getCurrentSqlParserConfig(sqlDialect);
        SqlParser sqlParser = SqlParser.create(sqlContent, parserConfig);
        SqlNodeList sqlNodeList = sqlParser.parseStmtList();
        List<SqlNode> sqlNodeInfoList = new ArrayList<>();
        sqlNodeInfoList.addAll(Arrays.asList(sqlNodeList.toArray()));
        return sqlNodeInfoList;
    }

    public static SqlParser.Config getCurrentSqlParserConfig(SqlDialect sqlDialect) {
        SqlConformance conformance = getSqlConformance(sqlDialect);
        return SqlParser.config()
                .withParserFactory(FlinkSqlParserFactories.create(conformance))
                .withConformance(conformance)
                .withLex(Lex.JAVA)
                .withIdentifierMaxLength(256);
    }

    public static FlinkSqlConformance getSqlConformance(SqlDialect sqlDialect) {
        switch (sqlDialect) {
            case HIVE:
                return FlinkSqlConformance.HIVE;
            case DEFAULT:
                return FlinkSqlConformance.DEFAULT;
            default:
                throw new TableException("Unsupported SQL dialect: " + sqlDialect);
        }
    }
}
