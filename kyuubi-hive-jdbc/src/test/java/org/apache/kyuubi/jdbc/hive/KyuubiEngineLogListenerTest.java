/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.sql.Connection;
import java.sql.Driver;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kyuubi.jdbc.KyuubiHiveDriver;
import org.apache.kyuubi.jdbc.hive.logs.KyuubiEngineLogListener;
import org.junit.Assert;
import org.junit.Test;

/** Created by tatian on 2022/6/24. */
public class KyuubiEngineLogListenerTest {

  @Test
  public void testCreateEngineLogListeners() {
    String classes =
        String.join(
            ",",
            TestKyuubiEngineLogListenersT1.class.getName(),
            TestKyuubiEngineLogListenersT2.class.getName());
    Properties info = new Properties();
    info.setProperty(KyuubiConnection.KYUUBI_ENGINE_LOG_LISTENER_CLASSES, classes);
    try {
      KyuubiEngineLogListener[] listeners =
          KyuubiConnection.tryCreateKyuubiEngineLogListeners(info);
      Assert.assertEquals(listeners.length, 2);
      Assert.assertTrue(listeners[0] instanceof TestKyuubiEngineLogListenersT1);
      Assert.assertTrue(listeners[1] instanceof TestKyuubiEngineLogListenersT2);
    } catch (Exception ex) {
      // unexpected
      ex.printStackTrace();
    }
  }

  @Test
  public void testCreateEngineLogListenersFailed() {
    Properties info = new Properties();
    info.setProperty(KyuubiConnection.KYUUBI_ENGINE_LOG_LISTENER_CLASSES, "non existed class");
    try {
      // will failed first
      KyuubiConnection connection = new KyuubiConnection("", info);
    } catch (Exception ex) {
      Assert.assertTrue(
          ex.getMessage()
              .contains(
                  "Failed to initialized engine log listener: java.lang.ClassNotFoundException: non existed class"));
    }
  }

  @Test
  public void testListenerRegister() {
    String classes =
        String.join(
            ",",
            TestKyuubiEngineLogListenersT1.class.getName(),
            TestKyuubiEngineLogListenersT2.class.getName());
    Properties info = new Properties();
    info.setProperty(KyuubiConnection.KYUUBI_ENGINE_LOG_LISTENER_CLASSES, classes);

    AtomicBoolean registered = new AtomicBoolean(false);
    KyuubiEngineLogListener userSideListener =
        new KyuubiEngineLogListener() {

          @Override
          public void onListenerRegistered(KyuubiConnection kyuubiConnection) {}

          @Override
          public void onLogFetchSuccess(List<String> logs) {
            registered.set(true);
          }

          @Override
          public void onLogFetchFailure(Throwable exception) {}
        };

    UserCustomizedExternalLogListenersPool.userControlledListeners.set(userSideListener);
    try {
      // will failed first
      Driver driver = (Driver) Class.forName(KyuubiHiveDriver.class.getName()).newInstance();
      Connection expectedFailed = driver.connect("jdbc:hive2://localhost:10009/default", info);
    } catch (Exception ex) {
      // expected
    }
    UserCustomizedExternalLogListenersPool.userControlledListeners.remove();

    Assert.assertTrue(registered.get());
  }

  private static class UserCustomizedExternalLogListenersPool {
    private static ThreadLocal<KyuubiEngineLogListener> userControlledListeners =
        new ThreadLocal<>();
  }

  public abstract static class BaseTestKyuubiEngineLogListeners implements KyuubiEngineLogListener {

    private KyuubiEngineLogListener userListener;

    public BaseTestKyuubiEngineLogListeners() {
      userListener = UserCustomizedExternalLogListenersPool.userControlledListeners.get();
      userListener.onLogFetchSuccess(null);
    }

    public void onListenerRegistered(KyuubiConnection kyuubiConnection) {}

    public void onLogFetchSuccess(List<String> logs) {
      if (userListener != null) {
        userListener.onLogFetchSuccess(logs);
      }
    }

    public void onLogFetchFailure(Throwable exception) {
      if (userListener != null) {
        userListener.onLogFetchFailure(exception);
      }
    }
  }

  public static class TestKyuubiEngineLogListenersT1 extends BaseTestKyuubiEngineLogListeners {}

  public static class TestKyuubiEngineLogListenersT2 extends BaseTestKyuubiEngineLogListeners {}
}
