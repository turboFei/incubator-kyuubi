/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.jdbc.hive;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KyuubiConnection extends HiveConnection {
    public static final Logger LOG = LoggerFactory.getLogger(KyuubiConnection.class.getName());

    // launch backend engine asynchronously
    static final String LAUNCH_ENGINE_ASYNC = "kyuubi.session.engine.launch.async";

    // the key to indicate that this TExecuteStatementReq is dedicated for kyuubi defined operation
    static final String DEFINED_OPERATION_ENABLED =
      "kyuubi.execute.statement.defined.operation.enabled";
    // kyuubi defined operation type
    static final String DEFINED_OPERATION_TYPE =
      "kyuubi.execute.statement.defined.operation.type";

    // to get launch engine operation handle
    static final String LAUNCH_ENGINE_TYPE =
      "LAUNCH_ENGINE";


    static final String INIT_FILE = "initFile";
    static final String DELAY_TO_INIT_FILE = "delayToInitFile";

    static final Long ENGINE_LOG_THREAD_DELAY_EXIT = 10 * 1000L;

    public KyuubiConnection(String url, Properties info) throws SQLException {
        super(Optional.of(url).map(u -> {
            try {
                if (url.contains(";" + INIT_FILE + "=")) {
                    u = u.replace(";" + INIT_FILE + "=", ";" + DELAY_TO_INIT_FILE + "=");
                } else if (url.contains("?" + INIT_FILE + "=")) {
                    u = u.replace("?" + INIT_FILE + "=", ";" + DELAY_TO_INIT_FILE + "=");
                } else if (url.contains("#" + INIT_FILE + "=")) {
                    u = u.replace("#" + INIT_FILE + "=", "#" + DELAY_TO_INIT_FILE + "=");
                }

                if (u.contains("#")) {
                    u += ";" + LAUNCH_ENGINE_ASYNC + "=true";
                } else {
                    u += "#" + LAUNCH_ENGINE_ASYNC + "=true";
                }
            } catch (Exception e) {
                LOG.error("Error when processing original url:" + u, e);
            }
            return u;
        }).get(), info);

        try {
            getLaunchEngineLog();
            executeDelayedInitFile(url, info);
        } catch (Exception e) {
            throw new SQLException("Failed to execute kyuubi connection", e);
        }
    }

    private void getLaunchEngineLog() throws Exception {
        LOG.info("Starting to get launch engine log.");

        Field sessionHandleField = HiveConnection.class.getDeclaredField("sessHandle");
        sessionHandleField.setAccessible(true);
        TSessionHandle sessionHandle = (TSessionHandle) sessionHandleField.get(this);

        Field clientField = HiveConnection.class.getDeclaredField("client");
        clientField.setAccessible(true);
        TCLIService.Iface client = (TCLIService.Iface) clientField.get(this);

        TExecuteStatementReq req = new TExecuteStatementReq();
        req.setSessionHandle(sessionHandle);
        Map<String, String> execStmtConf = new HashMap();
        execStmtConf.put(DEFINED_OPERATION_ENABLED, "true");
        execStmtConf.put(DEFINED_OPERATION_TYPE, LAUNCH_ENGINE_TYPE);
        req.setConfOverlay(execStmtConf);
        req.setStatement("PLACE_HOLDER");

        TOperationHandle launchEngineOpHandle = null;
        try {
            TExecuteStatementResp resp = client.ExecuteStatement(req);
            launchEngineOpHandle = resp.getOperationHandle();
        } catch (TException e) {
            LOG.error("Error when getting launch engine operation handle", e);
            return;
        }

        TOperationHandle finalLaunchEngineOpHandle = launchEngineOpHandle;
        Thread logThread = new Thread("engine-launch-log") {
            long timeToExit = Long.MAX_VALUE;
            long completeTime = 0;
            boolean continueToGetLog = true;
            @Override
            public void run() {
                try {
                    while (continueToGetLog && System.currentTimeMillis() < timeToExit) {
                        List<String> logs = getEngineLogs(finalLaunchEngineOpHandle, client);
                        if (logs.isEmpty() && System.currentTimeMillis() > completeTime) {
                            continueToGetLog = false;
                        }
                        for (String log: logs) {
                            LOG.info(log);
                        }
                        if (launchEngineCompleted(finalLaunchEngineOpHandle, client)) {
                            completeTime = System.currentTimeMillis();
                            timeToExit = completeTime + ENGINE_LOG_THREAD_DELAY_EXIT;
                        }
                        Thread.sleep(300);
                    }
                } catch (Exception e) {
                    // do nothing
                }
                LOG.info("Finished to get engine launch log.");
            }
        };
        logThread.start();
    }

    private boolean launchEngineCompleted(TOperationHandle opHandle, TCLIService.Iface client) {
        TGetOperationStatusReq getOperationStatusReq = new TGetOperationStatusReq(opHandle);

        try {
            TGetOperationStatusResp getOperationStatusResp = client.GetOperationStatus(getOperationStatusReq);
            return getOperationStatusResp.getOperationCompleted() != 0;
        } catch (TException e) {
            return true;
        }
    }

    private List<String> getEngineLogs(TOperationHandle opHandle, TCLIService.Iface client) throws SQLException {
        int fetchSize = HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE.defaultIntVal;
        TFetchResultsReq tFetchResultsReq = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_NEXT, fetchSize);
        tFetchResultsReq.setFetchType((short) 1);

        List<String> logs = new ArrayList<>();
        try {
            TFetchResultsResp tFetchResultsResp = client.FetchResults(tFetchResultsReq);
            RowSet rowSet = RowSetFactory.create(tFetchResultsResp.getResults(), this.getProtocol());
            for (Object[] row: rowSet) {
                logs.add(String.valueOf(row[0]));
            }
        } catch (TException e) {
            throw new SQLException("Error building result set for query log", e);
        }
        return Collections.unmodifiableList(logs);
    }

    private static String buildJdbcUrlWithConf(
      String url,
      Map<String, String> sessionConf,
      Map<String, String> jdbcConfig,
      Map<String, String> jdbcVars) {
        String sessionConfStr = map2KVString(sessionConf);
        String jdbcConfStr = "";
        if (!jdbcConfig.isEmpty()) {
            jdbcConfStr = "?" + map2KVString(jdbcConfig);
        }
        String jdbcVarsStr = "";
        if (!jdbcVars.isEmpty()) {
            jdbcVarsStr = "#" + map2KVString(jdbcVars);
        }
        return url + sessionConfStr + jdbcConfStr + jdbcVarsStr;
    }

    private static String map2KVString(Map<String, String> map) {
        return String.join(";", map.entrySet().stream()
          .map(kv -> kv.getKey() + "=" + kv.getValue()).collect(Collectors.toList()));
    }

    private static JdbcConnectionParams parseConnectionParams(String url, Properties info) throws Exception {
        Method parseURLMethod = Utils.class.getDeclaredMethod(
          "parseURL", String.class, Properties.class);
        parseURLMethod.setAccessible(true);
        JdbcConnectionParams connParams = (JdbcConnectionParams) parseURLMethod.invoke(
          null, url, info);
        return connParams;
    }

    private void executeDelayedInitFile(String url, Properties info) throws Exception {
        JdbcConnectionParams connParams = parseConnectionParams(url, info);
        String initFile = connParams.getSessionVars().get(INIT_FILE);
        Field initFileField = HiveConnection.class.getDeclaredField("initFile");
        initFileField.setAccessible(true);
        initFileField.set(this, initFile);

        Method executeInitFileMethod = HiveConnection.class.getDeclaredMethod("executeInitSql");
        executeInitFileMethod.setAccessible(true);
        executeInitFileMethod.invoke(this);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
        try {
            Field clientField = HiveConnection.class.getDeclaredField("client");
            clientField.setAccessible(true);
            TCLIService.Iface client = (TCLIService.Iface) clientField.get(this);
            Field handleField = HiveConnection.class.getDeclaredField("sessHandle");
            handleField.setAccessible(true);
            TSessionHandle sessionHandle = (TSessionHandle) handleField.get(this);
            return new KyuubiDatabaseMetaData(this, client, sessionHandle);
        } catch (NoSuchFieldException | IllegalAccessException rethrow) {
            throw new RuntimeException(rethrow);
        }
    }
}
