//package org.apache.flink.table.planner.runtime.stream.sql;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ArrayNode;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import org.apache.calcite.config.Lex;
//import org.apache.calcite.sql.SqlNode;
//import org.apache.calcite.sql.SqlNodeList;
//import org.apache.calcite.sql.parser.SqlParseException;
//import org.apache.calcite.sql.parser.SqlParser;
//import org.apache.calcite.sql.validate.SqlConformance;
//import org.apache.commons.cli.CommandLine;
//import org.apache.commons.cli.CommandLineParser;
//import org.apache.commons.cli.DefaultParser;
//import org.apache.commons.cli.Options;
//import org.apache.commons.io.FileUtils;
//import org.apache.commons.io.IOUtils;
//
//import org.apache.flink.api.dag.Transformation;
//import org.apache.flink.client.deployment.application.UnsuccessfulExecutionException;
//import org.apache.flink.client.program.ProgramAbortException;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.configuration.CoreOptions;
//import org.apache.flink.configuration.TaskManagerOptions;
//import org.apache.flink.core.fs.FileSystem;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.core.fs.UnsupportedFileSystemSchemeException;
//import org.apache.flink.runtime.clusterframework.ApplicationStatus;
//import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
//import org.apache.flink.runtime.jobgraph.JobGraph;
//import org.apache.flink.runtime.jobgraph.RestoreMode;
//import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
//import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
//import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerUtils;
//
//import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JacksonException;
//
//import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.graph.StreamGraph;
//import org.apache.flink.streaming.api.graph.StreamNode;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.SqlDialect;
//import org.apache.flink.table.api.SqlParserException;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableConfig;
//import org.apache.flink.table.api.TableException;
//import org.apache.flink.table.api.ValidationException;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
//import org.apache.flink.table.api.config.TableConfigOptions;
//import org.apache.flink.table.operations.ModifyOperation;
//import org.apache.flink.table.operations.Operation;
//import org.apache.flink.table.operations.command.SetOperation;
//import org.apache.flink.table.operations.ddl.CreateViewOperation;
//import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
//import org.apache.flink.util.ExceptionUtils;
//import org.apache.flink.util.StringUtils;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.net.URI;
//import java.net.URL;
//import java.nio.charset.StandardCharsets;
//import java.util.ArrayList;
//import java.util.Enumeration;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//
//import static org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec;
//import static org.apache.flink.runtime.jobgraph.SavepointConfigOptions.RESTORE_MODE;
//import static org.apache.flink.runtime.jobgraph.SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE;
//import static org.apache.flink.runtime.jobgraph.SavepointConfigOptions.SAVEPOINT_PATH;
//import static org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION;
//import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DATABASE_NAME;
//import static org.apache.flink.table.api.config.TableConfigOptions.USE_EXTERNAL_METASTORE;
//
//public class FlinkCustomizedSqlJobClient {
//    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlJobClient.class);
//    private StreamExecutionEnvironment streamExecutionEnvironment;
//    private StreamTableEnvironmentImpl streamTableEnvironment;
//    private SqlDialect currentSqlDialect = SqlDialect.DEFAULT;
//
//    private StreamGraph streamGraph;
//
//    private JobGraph jobGraph;
//
//    private ValidateResultListener validateResultListener;
//    private String validateResultPath;
//    private ValidateResultPersistService validateResultPersistService;
//    private Configuration jobConfig;
//    private String currentSqlPos;
//
//    private String passedInDag = "";
//    // 获取AppClassLoader 从classpath下获得所有的资源
//    private final ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
//
//    private final List<ModifyOperation> modifyOperations = new ArrayList<>();
//
//    public void initEnvironment(String mode, boolean isStream) {
//        Configuration configuration = new Configuration();
//        if (mode.equalsIgnoreCase("local")) {
//            configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
//            streamExecutionEnvironment =
//                    StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
//            streamExecutionEnvironment.getCheckpointConfig().disableCheckpointing();
//            streamExecutionEnvironment.enableCheckpointing(
//                    60 * 1000, CheckpointingMode.EXACTLY_ONCE);
//
//        } else {
//            streamExecutionEnvironment =
//                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
//        }
//
//        String useDewuTableEnv =
//                System.getenv().getOrDefault(USE_EXTERNAL_METASTORE.key(), "false");
//        String clusterName = System.getenv().getOrDefault("CLUSTER_NAME", "");
//        String taskProject = System.getenv().getOrDefault("TASK_PROJECT", "").replace("-", "_");
//
//        initHiveConfig(clusterName);
//
//        EnvironmentSettings environmentSettings =
//                isStream
//                        ? EnvironmentSettings.newInstance()
//                        .inStreamingMode()
//                        .withBuiltInDatabaseName(
//                                StringUtils.isNullOrWhitespaceOnly(taskProject)
//                                        || "false".equals(useDewuTableEnv)
//                                        ? TABLE_DATABASE_NAME.defaultValue()
//                                        : taskProject)
//                        .build()
//                        : EnvironmentSettings.newInstance().inBatchMode().build();
//
//        LOG.info("useDewuTableEnv value is {}", useDewuTableEnv);
//        streamTableEnvironment =
//                "true".equals(useDewuTableEnv)
//                        ? (StreamTableEnvironmentImpl)
//                        DewuStreamTableEnviroment.create(
//                                streamExecutionEnvironment, environmentSettings)
//                        : (StreamTableEnvironmentImpl)
//                        StreamTableEnvironment.create(
//                                streamExecutionEnvironment, environmentSettings);
//
//        initTableConfig();
//
//        this.jobConfig = ((Configuration) streamExecutionEnvironment.getConfiguration());
//        this.validateResultPath = jobConfig.get(EXECUTION_VALIDATE_RESULT_DIR);
//        this.validateResultListener = new ValidateResultListener();
//        LOG.info("configed result storage path is {}", this.validateResultPath);
//        String taskId = jobConfig.getString("execution.libra.taskid", "");
//        String instanceId = jobConfig.getString("execution.libra.instid", "");
//        if (!StringUtils.isNullOrWhitespaceOnly(validateResultPath)) {
//            try {
//                validateResultPersistService =
//                        new ValidateResultPersistService(
//                                ValidateResultPersistService.getClusterStoragePath(
//                                        validateResultPath, taskId, instanceId));
//            } catch (IOException e) {
//                LOG.error("create validate result persist service failed", e);
//            }
//        }
//    }
//
//    public void initHiveConfig(String clusterName) {
//        File hiveConfFile = new File("/tmp/hive-site.xml");
//        try {
//            hiveConfFile.createNewFile();
//        } catch (Exception e) {
//            LOG.error("create /tmp/hive-site.xml failed", e);
//        }
//
//        String hiveConfSource =
//                clusterName.contains("prd")
//                        ? "hiveconf/hive-site-prd.xml"
//                        : "hiveconf/hive-site-test.xml";
//
//        try (InputStream inputStream =
//                     Thread.currentThread()
//                             .getContextClassLoader()
//                             .getResourceAsStream(hiveConfSource);
//             OutputStream outputStream = new FileOutputStream("/tmp/hive-site.xml")) {
//            byte[] buffer = new byte[1024];
//            int length;
//            while ((length = inputStream.read(buffer)) != -1) {
//                outputStream.write(buffer, 0, length);
//            }
//        } catch (Exception e) {
//            LOG.error("write /tmp/hive-site.xml failed", e);
//        }
//    }
//
//    public void initTableConfig() {
//        Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
//        configuration.setString("table.exec.source.idle-timeout", "30 s");
//        configuration.setString(IDLE_STATE_RETENTION.key(), "36 hours");
//        configuration.setString(TableConfigOptions.LOCAL_TIME_ZONE, "Asia/Shanghai");
//        configuration.setString("table.exec.sink.upsert-materialize", "NONE");
//        configuration.setString("table.exec.legacy-cast-behaviour", "enabled");
//
//        PlanData oldPlanData = null;
//        try {
//            if (passedInDag != null && passedInDag.length() > 0) {
//                oldPlanData = PlanData.parseFromText(passedInDag);
//
//                if (oldPlanData != null) {
//                    for (PlanStreamNode node : oldPlanData.nodes) {
//                        if (node.stateTtl != null && node.execNodeId != null) {
//                            LOG.info(
//                                    "setting the node {} ttl to {}",
//                                    node.execNodeId,
//                                    node.stateTtl);
//                            configuration.setLong(
//                                    "libraTTLConfig" + node.execNodeId, node.stateTtl);
//                        }
//                    }
//                }
//            }
//        } catch (Exception ignored) {
//
//        }
//
//        TableConfig tableConfig = streamTableEnvironment.getConfig();
//
//        Map<String, String> tableConfMap = System.getenv();
//        for (Map.Entry<String, String> entry : tableConfMap.entrySet()) {
//            if (entry.getKey().startsWith("FLINK_") || entry.getKey().startsWith("ADAPTER_")) {
//                continue;
//            } else {
//                configuration.setString(entry.getKey(), entry.getValue());
//                tableConfig.addJobParameter(entry.getKey(), entry.getValue());
//                LOG.info("set table config {}={}", entry.getKey(), entry.getValue());
//            }
//        }
//    }
//
//    public void registerFunctions() throws IOException {
//
//        Enumeration<URL> environmentConfigEnumeration =
//                systemClassLoader.getResources("sql-client-defaults.yaml");
//
//        while (environmentConfigEnumeration.hasMoreElements()) {
//            URL environmentConfigURL = environmentConfigEnumeration.nextElement();
//            LOG.info("find environmentConfig in " + environmentConfigURL.toString());
//
//            // create user-defined functions
//            udfList.applyCustomUdf(streamTableEnvironment.getConfig().getConfiguration())
//                    .forEach(
//                            (name, functionClass) -> {
//                                streamTableEnvironment.createFunction(name, functionClass, true);
//                                LOG.info(
//                                        "streamTableEnvironment createTemporaryFunction {} : {}",
//                                        name,
//                                        functionClass.getCanonicalName());
//                            });
//
//            LOG.info(
//                    "UserDefinedFunctions : \n{}",
//                    String.join(",\n", streamTableEnvironment.listUserDefinedFunctions()));
//        }
//    }
//
//    public void execute(String sqlContent) throws IOException {
//        long startTime = System.currentTimeMillis();
//        if (sqlContent.length() > 0) {
//            executeSql(sqlContent, this.currentSqlDialect);
//        }
//
//        LOG.info("sql execute cost {} ms", System.currentTimeMillis() - startTime);
//
//        // 编译 DAG
//        try {
//            long startTranslateTime = System.currentTimeMillis();
//            List<Transformation<?>> transformations =
//                    streamTableEnvironment.getPlanner().translate(modifyOperations);
//            LOG.info(
//                    "transformations translate cost {} ms",
//                    System.currentTimeMillis() - startTranslateTime);
//
//            for (Transformation<?> transformation : transformations) {
//                streamExecutionEnvironment.addOperator(transformation);
//            }
//        } catch (Exception exception) {
//            LOG.error("translate modifyOperations cause exception", exception);
//            throw new RuntimeException(exception);
//        }
//
//        PlanData oldPlanData = null;
//        try {
//            if (passedInDag != null && passedInDag.length() > 0) {
//                oldPlanData = PlanData.parseFromText(passedInDag);
//            }
//        } catch (JacksonException e) {
//            LOG.error("parse plan error", e);
//            throw new RuntimeException(e);
//        }
//
//        if (oldPlanData != null) {
//            Map<String, PlanStreamNode> planMapping = oldPlanData.generateNodeHash();
//            this.streamGraph = streamExecutionEnvironment.getStreamGraph(planMapping);
//        } else {
//            this.streamGraph = streamExecutionEnvironment.getStreamGraph();
//        }
//
//        this.jobGraph = streamGraph.getJobGraph();
//        PlanData planData = new PlanData(this.streamGraph, this.jobGraph);
//        JSONObject jsonObj = (JSONObject) JSON.toJSON(planData);
//        LOG.info("job plan modified {}", jsonObj.toJSONString());
//    }
//
//    public void executeSql(String sqlContent, SqlDialect sqlDialect) {
//        currentSqlPos = "no specific SQL related";
//        if (sqlContent.trim().length() > 0) {
//            try {
//                List<SqlNode> sqlPartList = splitSqlContext(sqlContent, sqlDialect);
//                if (sqlDialect.equals(SqlDialect.DEFAULT)) {
//                    executeFlinkSql(sqlPartList);
//                } else {
//                    throw new RuntimeException("SqlDialect " + sqlDialect + " not support yet");
//                }
//            } catch (SqlParseException sqlParseException) {
//                // SQL text parsing failure, return line number
//                throw new SqlValidateException("parsing SQL failed: " + sqlParseException);
//            } catch (ValidationException validationException) {
//                throw new SqlValidateException(
//                        String.format("%s caused %s", currentSqlPos, validationException));
//            } catch (Exception e) {
//                throw new SqlExecutionException(sqlContent, e);
//            }
//        }
//        currentSqlPos = "SQL parse ended";
//    }
//
//    public void executeFlinkSql(List<SqlNode> sqlPartList) {
//
//        for (SqlNode sqlPart : sqlPartList) {
//            this.currentSqlPos = ValidateErrorUtils.ErrorSqlNodePos(sqlPart);
//            List<Operation> operations = null;
//            try {
//                operations = streamTableEnvironment.getParser().parse(sqlPart.toString());
//            } catch (SqlParserException sqlParserException) {
//                throw new RuntimeException(sqlParserException);
//            }
//            if (operations != null && operations.size() == 1) {
//                Operation operation = operations.get(0);
//                if (operation instanceof CreateViewOperation) {
//                    CreateViewOperation createViewOperation = (CreateViewOperation) operation;
//                    String query = createViewOperation.getCatalogView().getExpandedQuery();
//                    String viewName = createViewOperation.getViewIdentifier().getObjectName();
//                    Table table = streamTableEnvironment.sqlQuery(query);
//                    streamTableEnvironment.createTemporaryView(viewName, table);
//                } else if (operation instanceof ModifyOperation) {
//                    ModifyOperation modifyOperation = (ModifyOperation) operation;
//                    modifyOperations.add(modifyOperation);
//                } else if (operation instanceof SetOperation) {
//                    Configuration newConfig = new Configuration();
//                    SetOperation setOperation = (SetOperation) operation;
//                    String key = setOperation.getKey().get().trim();
//                    String value = setOperation.getValue().get().trim();
//                    if (key.equalsIgnoreCase("execution.runtime-mode")
//                            && value.equalsIgnoreCase("batch")) {
//                        if (sqlPartList.indexOf(sqlPart) != 0) {
//                            throw new UnsupportedOperationException(
//                                    "set execution.runtime-mode only supported in the first row");
//                        }
//                        initEnvironment("kubernetes", false);
//                    }
//                    newConfig.setString(key, value);
//                    streamExecutionEnvironment.configure(newConfig);
//                } else {
//                    streamTableEnvironment.executeSql(sqlPart.toString()).print();
//                }
//            } else {
//                throw new RuntimeException("sql not support yet. \n" + currentSqlPos);
//            }
//        }
//    }
//
//    public void streamGraphConfig(String planJson) {
//        try {
//            if (planJson != null && planJson.trim().length() > 0) {
//                LOG.info("apply planJson :\n" + planJson);
//                ObjectMapper objectMapper = new ObjectMapper();
//                ObjectNode planJsonObject;
//                JsonNode jsonNode = objectMapper.readTree(planJson);
//                HashMap<String, ObjectNode> planMaps = new HashMap<>();
//                if (jsonNode.isObject()) {
//                    planJsonObject = (ObjectNode) jsonNode;
//                    ArrayNode nodes = (ArrayNode) planJsonObject.get("nodes");
//                    for (int i = 0; i < nodes.size(); i++) {
//                        ObjectNode planNode = (ObjectNode) nodes.get(i);
//                        String contents = planNode.get("contents").asText();
//                        planMaps.put(contents, planNode);
//                    }
//
//                    for (StreamNode streamNode : streamGraph.getStreamNodes()) {
//                        String operatorName = streamNode.getOperatorName();
//                        ObjectNode planNode = planMaps.get(operatorName);
//                        int parallelism = planNode.get("parallelism").asInt();
//                        streamNode.setParallelism(parallelism);
//                    }
//                }
//            }
//        } catch (Exception e) {
//            throw new ValidationException(planJson, e);
//        }
//    }
//
//    public void setSavepointRestoreSettings(String savepointPath, Boolean allowNonRestoredState) {
//        if (savepointPath != null) {
//            streamGraph.setSavepointRestoreSettings(
//                    SavepointRestoreSettings.forPath(savepointPath, allowNonRestoredState));
//            LOG.info(
//                    "flink job will restore from {} with allowNonRestoredState []",
//                    savepointPath,
//                    allowNonRestoredState.toString());
//        }
//    }
//
//    public void deployOrValidate() throws Exception {
//        // 提交任务之前的DAG配置
//        if (streamTableEnvironment
//                .getConfig()
//                .getConfiguration()
//                .getBoolean("table.exec.split-slot-sharing-group-per-vertex", false)) {
//            // 是否为每个VERTEX分配单独的SLOT，默认为false。
//            for (StreamNode streamNode : streamGraph.getStreamNodes()) {
//                if (streamNode.getInEdges() == null || streamNode.getInEdges().isEmpty()) {
//                    streamNode.setSlotSharingGroup(streamNode.getOperatorName());
//                    LOG.info(
//                            "set source node {} slot sharing group name",
//                            streamNode.getOperatorName());
//                }
//            }
//        }
//
//        if (!StringUtils.isNullOrWhitespaceOnly(validateResultPath)) {
//            String savepointRestorePath = jobConfig.get(SAVEPOINT_PATH);
//            Boolean ignoreUnclaimedState = jobConfig.get(SAVEPOINT_IGNORE_UNCLAIMED_STATE);
//            RestoreMode restoreMode = jobConfig.get(RESTORE_MODE);
//            if (!StringUtils.isNullOrWhitespaceOnly(savepointRestorePath)) {
//                CheckpointValidator checkpointValidator =
//                        new CheckpointValidator(
//                                jobGraph, savepointRestorePath, restoreMode, ignoreUnclaimedState);
//                CheckpointValidator.CheckpointValidateResult checkpointRestorable =
//                        checkpointValidator.isCheckpointRestorable();
//
//                validateResultListener.saveCheckpointRestorable(checkpointRestorable);
//            }
//            Map<SlotResourceSpec, Integer> resourcesCounter;
//            if (jobGraph.getJobResourceInformation() == null) {
//                final WorkerResourceSpec defaultWorkerResourceSpec =
//                        KubernetesWorkerResourceSpecFactory.INSTANCE
//                                .createDefaultWorkerResourceSpec(jobConfig);
//                int numSlotsPerWorker = jobConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
//                ResourceProfile resourceProfile =
//                        SlotManagerUtils.generateDefaultSlotResourceProfile(
//                                defaultWorkerResourceSpec, numSlotsPerWorker);
//                LOG.info(
//                        "TaskExecutorProcessSpec resources is {}",
//                        processSpecFromWorkerResourceSpec(jobConfig, defaultWorkerResourceSpec));
//
//                JobResourceInformation jobResourceInformation =
//                        new JobResourceInformation(
//                                getSimpleJobResourceProfile(jobConfig),
//                                Lists.newArrayList(
//                                        SlotResourceSpec.create(
//                                                defaultWorkerResourceSpec,
//                                                resourceProfile,
//                                                "default")));
//                resourcesCounter = jobResourceInformation.getTMResourceSpecNumber();
//
//            } else {
//                resourcesCounter = jobGraph.getJobResourceInformation().getTMResourceSpecNumber();
//            }
//
//            validateResultListener.saveResourceCounter(resourcesCounter, jobConfig);
//        } else {
//            // deploy
//            streamExecutionEnvironment.execute(streamGraph);
//        }
//    }
//
//    public void stopJobClientAndDeleteCluster() {
//        FlinkKubeClient client =
//                FlinkKubeClientFactory.getInstance()
//                        .fromConfiguration(jobConfig, "clientValidator");
//        String clusterId = jobConfig.getString("kubernetes.cluster-id", "");
//        LOG.info("validate job finish , cleaning {}", clusterId);
//        client.stopAndCleanupCluster(clusterId);
//    }
//
//    public static void main(String[] args) throws Exception {
//        LOG.info("FlinkSqlJobClient main args [{}] ", String.join(" ", args));
//        Options options = FlinkSqlJobOptions.getFlinkSqlJobOptions();
//
//        CommandLineParser commandLineParser = new DefaultParser();
//        CommandLine cmd = commandLineParser.parse(options, args);
//
//        FlinkSqlJobClient flinkSqlJobClient = new FlinkSqlJobClient();
//
//        Map<String, String> tableConfMap = System.getenv();
//        boolean isStream = true;
//        if (tableConfMap.get("execution.runtime-mode") == null
//                || tableConfMap.get("execution.runtime-mode").equalsIgnoreCase("stream")) {
//            isStream = true;
//        } else {
//            isStream = false;
//        }
//        flinkSqlJobClient.initEnvironment(
//                cmd.getOptionValue(FlinkSqlJobOptions.MODE.getOpt(), "kubernetes"), isStream);
//
//        try {
//            flinkSqlJobClient.registerFunctions();
//
//            if (cmd.hasOption(FlinkSqlJobOptions.DAG_CHANGED.getOpt())) {
//                String dagPath = cmd.getOptionValue(FlinkSqlJobOptions.DAG_CHANGED.getOpt());
//                String dagContent = getResourceContent(dagPath);
//                LOG.info("dag passed in: \n" + dagContent);
//                flinkSqlJobClient.passedInDag = dagContent.trim();
//            } else {
//                LOG.info("no dag passed in");
//                flinkSqlJobClient.passedInDag = "";
//            }
//
//            // 执行用户的业务逻辑sql
//            if (cmd.hasOption(FlinkSqlJobOptions.SQL_PATH.getOpt())) {
//                String sqlPath = cmd.getOptionValue(FlinkSqlJobOptions.SQL_PATH.getOpt());
//                String sqlContent = getResourceContent(sqlPath);
//                LOG.info("sqlContent: \n" + sqlContent);
//                flinkSqlJobClient.execute(sqlContent);
//            } else {
//                throw new RuntimeException(
//                        String.format(
//                                "param %s or %s is required",
//                                FlinkSqlJobOptions.SQL_PATH.getOpt(),
//                                FlinkSqlJobOptions.JOB_CONTENT.getOpt()));
//            }
//
//            // 从savepoint中恢复状态数据
//            if (cmd.hasOption(FlinkSqlJobOptions.FORM_SAVEPOINT.getOpt())) {
//                String savepointPath =
//                        cmd.getOptionValue(FlinkSqlJobOptions.FORM_SAVEPOINT.getOpt());
//                boolean allowNonRestoredState =
//                        Boolean.parseBoolean(
//                                cmd.getOptionValue(
//                                        FlinkSqlJobOptions.ALLOW_NON_RESTORED_STATE.getOpt(),
//                                        "false"));
//                flinkSqlJobClient.setSavepointRestoreSettings(savepointPath, allowNonRestoredState);
//            }
//
//            flinkSqlJobClient.deployOrValidate();
//        } catch (Throwable e) {
//            Optional<ApplicationStatus> isCanceled =
//                    ExceptionUtils.findThrowable(e, UnsuccessfulExecutionException.class)
//                            .map(UnsuccessfulExecutionException::getStatus)
//                            .filter(ApplicationStatus.CANCELED::equals);
//            if (isCanceled.isPresent()) {
//                LOG.info("Flink SQL Job Client received job cancel. ignore.");
//                return;
//            }
//            if (ExceptionUtils.findThrowable(e, ProgramAbortException.class).isPresent()) {
//                LOG.warn("Flink SQL Job Client received program abort. ignore.");
//                return;
//            }
//            LOG.error("Flink SQL Job Client received error", e);
//            flinkSqlJobClient.validateResultListener.registerParseFail(e);
//            if (StringUtils.isNullOrWhitespaceOnly(flinkSqlJobClient.validateResultPath)) {
//                throw e;
//            }
//        } finally {
//            if (!StringUtils.isNullOrWhitespaceOnly(flinkSqlJobClient.validateResultPath)) {
//                try {
//                    if (flinkSqlJobClient.jobGraph != null) {
//                        if (flinkSqlJobClient.jobConfig.get(EXECUTION_JOB_GRAPH_STORE)) {
//                            flinkSqlJobClient.validateResultPersistService.storeJobGraph(
//                                    "jobGraph", flinkSqlJobClient.jobGraph);
//                        }
//                    }
//                    flinkSqlJobClient.validateResultPersistService.serializeValidateResultToOss(
//                            "validateResult",
//                            flinkSqlJobClient.validateResultListener.validateResult);
//                    Thread.sleep(1000L);
//                } catch (Exception e) {
//                    LOG.error("create jobGraph storage error", e);
//                }
//
//                flinkSqlJobClient.stopJobClientAndDeleteCluster();
//            }
//        }
//    }
//
//    public static String getResourceContent(String resourcePath) {
//        try {
//            URI sqlPathURI = new URI(resourcePath);
//            String scheme = sqlPathURI.getScheme();
//            if (scheme == null || scheme.equals("file") || scheme.equals("local")) {
//                return FileUtils.readFileToString(
//                        new File(sqlPathURI.getPath()), StandardCharsets.UTF_8);
//
//            } else if (scheme.equals("http")) {
//                return IOUtils.toString(sqlPathURI.toURL(), StandardCharsets.UTF_8);
//
//            } else if (scheme.equals("oss") || scheme.equals("hdfs")) {
//                InputStream inputStream = null;
//                try {
//                    FileSystem fileSystem = FileSystem.get(sqlPathURI);
//                    inputStream = fileSystem.open(new Path(resourcePath));
//                    return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
//                } catch (Throwable throwable) {
//                    throw new UnsupportedFileSystemSchemeException(resourcePath, throwable);
//                } finally {
//                    if (inputStream != null) {
//                        IOUtils.closeQuietly(inputStream);
//                    }
//                }
//            } else {
//                throw new UnsupportedFileSystemSchemeException(resourcePath);
//            }
//        } catch (Throwable throwable) {
//            throw new RuntimeException("获取资源失败 " + resourcePath, throwable);
//        }
//    }
//
//    public static List<SqlNode> splitSqlContext(String sqlContent, SqlDialect sqlDialect)
//            throws SqlParseException {
//        SqlParser.Config parserConfig = getCurrentSqlParserConfig(sqlDialect);
//        SqlParser sqlParser = SqlParser.create(sqlContent, parserConfig);
//        SqlNodeList sqlNodeList = sqlParser.parseStmtList();
//        List<SqlNode> sqlNodeInfoList = new ArrayList<>();
//        sqlNodeInfoList.addAll(sqlNodeList);
//        return sqlNodeInfoList;
//    }
//
//    public static SqlParser.Config getCurrentSqlParserConfig(SqlDialect sqlDialect) {
//        SqlConformance conformance = getSqlConformance(sqlDialect);
//        return SqlParser.config()
//                .withParserFactory(FlinkSqlParserFactories.create(conformance))
//                .withConformance(conformance)
//                .withLex(Lex.JAVA)
//                .withIdentifierMaxLength(256);
//    }
//
//    public static FlinkSqlConformance getSqlConformance(SqlDialect sqlDialect) {
//        switch (sqlDialect) {
//            case HIVE:
//                return FlinkSqlConformance.HIVE;
//            case DEFAULT:
//                return FlinkSqlConformance.DEFAULT;
//            default:
//                throw new TableException("Unsupported SQL dialect: " + sqlDialect);
//        }
//    }
//}
