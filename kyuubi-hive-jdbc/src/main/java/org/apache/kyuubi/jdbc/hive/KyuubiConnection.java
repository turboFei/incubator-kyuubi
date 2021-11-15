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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KyuubiConnection extends org.apache.hive.jdbc.KyuubiConnection {
    private static final Logger LOG = LoggerFactory.getLogger(KyuubiConnection.class.getName());

    // launch backend engine asynchronously
    public static final String LAUNCH_ENGINE_ASYNC = "kyuubi.session.engine.launch.async";

    // the key to indicate that this TExecuteStatementReq is dedicated for kyuubi defined operation
    public static final String DEFINED_OPERATION_ENABLED =
      "kyuubi.execute.statement.defined.operation.enabled";
    // kyuubi defined operation type
    public static final String DEFINED_OPERATION_TYPE =
      "kyuubi.execute.statement.defined.operation.type";
    public static final String DEFINED_OPERATION_STATEMENT = "PLACE_HOLDER";

    // to get launch engine operation handle
    public static final String LAUNCH_ENGINE_TYPE =
      "LAUNCH_ENGINE";


    static final String INIT_FILE = "initFile";
    static final String DELAY_TO_INIT_FILE = "delayToInitFile";

    static final Long ENGINE_LOG_THREAD_DELAY_EXIT = 10 * 1000L;

    public KyuubiConnection(String url, Properties info) throws SQLException {
        super(Optional.of(url).map(u -> {
            String INIT_FILE_PART = INIT_FILE + "=";
            String DELAY_TO_INIT_FILE_PART = DELAY_TO_INIT_FILE + "=";
            for (String delimiter: Arrays.asList(";", "?", "#")) {
                if (u.contains(delimiter + INIT_FILE_PART)) {
                    u = u.replace(delimiter + INIT_FILE_PART,
                      delimiter + DELAY_TO_INIT_FILE_PART);
                }
            }

            if (u.contains("#")) {
                u += ";" + LAUNCH_ENGINE_ASYNC + "=true";
            } else {
                u += "#" + LAUNCH_ENGINE_ASYNC + "=true";
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

        Field sessionHandleField = org.apache.hive.jdbc.KyuubiConnection.class.getDeclaredField("sessHandle");
        sessionHandleField.setAccessible(true);
        TSessionHandle sessionHandle = (TSessionHandle) sessionHandleField.get(this);

        Field clientField = org.apache.hive.jdbc.KyuubiConnection.class.getDeclaredField("client");
        clientField.setAccessible(true);
        TCLIService.Iface client = (TCLIService.Iface) clientField.get(this);

        TExecuteStatementReq req = new TExecuteStatementReq();
        req.setSessionHandle(sessionHandle);
        Map<String, String> execStmtConf = new HashMap();
        execStmtConf.put(DEFINED_OPERATION_ENABLED, "true");
        execStmtConf.put(DEFINED_OPERATION_TYPE, LAUNCH_ENGINE_TYPE);
        req.setConfOverlay(execStmtConf);
        req.setStatement(DEFINED_OPERATION_STATEMENT);

        TOperationHandle launchEngineOpHandle = null;
        try {
            TExecuteStatementResp resp = client.ExecuteStatement(req);
            if (!resp.getStatus().getStatusCode().equals(TStatusCode.SUCCESS_STATUS)) {
                // the service side does not support this kind of kyuubi defined operation
                return;
            }
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

    private void executeDelayedInitFile(String url, Properties info) throws Exception {
        Method parseURLMethod = Utils.class.getDeclaredMethod(
          "parseURL", String.class, Properties.class);
        parseURLMethod.setAccessible(true);
        JdbcConnectionParams connParams = (JdbcConnectionParams) parseURLMethod.invoke(
          null, url, info);
        String initFile = connParams.getSessionVars().get(INIT_FILE);
        Field initFileField = org.apache.hive.jdbc.KyuubiConnection.class.getDeclaredField("initFile");
        initFileField.setAccessible(true);
        initFileField.set(this, initFile);

        Method executeInitFileMethod = org.apache.hive.jdbc.KyuubiConnection.class.getDeclaredMethod("executeInitSql");
        executeInitFileMethod.setAccessible(true);
        executeInitFileMethod.invoke(this);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
        try {
            Field clientField = org.apache.hive.jdbc.KyuubiConnection.class.getDeclaredField("client");
            clientField.setAccessible(true);
            TCLIService.Iface client = (TCLIService.Iface) clientField.get(this);
            Field handleField = org.apache.hive.jdbc.KyuubiConnection.class.getDeclaredField("sessHandle");
            handleField.setAccessible(true);
            TSessionHandle sessionHandle = (TSessionHandle) handleField.get(this);
            return new KyuubiDatabaseMetaData(this, client, sessionHandle);
        } catch (NoSuchFieldException | IllegalAccessException rethrow) {
            throw new RuntimeException(rethrow);
        }
    }
}
