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

package yaooqinn.kyuubi.rsc.driver;

import java.io.File;
import java.lang.reflect.Method;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import yaooqinn.kyuubi.rsc.api.JobContext;

class JobContextImpl implements JobContext {

  private static final Logger LOG = LoggerFactory.getLogger(JobContextImpl.class);

  private final JavaSparkContext sc;
  private final File localTmpDir;
  private volatile SQLContext sqlctx;
  private volatile HiveContext hivectx;
  private final RSCDriver driver;
  private volatile Object sparksession;

  public JobContextImpl(JavaSparkContext sc, File localTmpDir, RSCDriver driver) {
    this.sc = sc;
    this.localTmpDir = localTmpDir;
    this.driver = driver;
  }

  @Override
  public JavaSparkContext sc() {
    return sc;
  }

  @Override
  public Object sparkSession() throws Exception {
    if (sparksession == null) {
      synchronized (this) {
        if (sparksession == null) {
          try {
            Class<?> clz = Class.forName("org.apache.spark.sql.SparkSession$");
            Object spark = clz.getField("MODULE$").get(null);
            Method m = clz.getMethod("builder");
            Object builder = m.invoke(spark);
            builder.getClass().getMethod("sparkContext", SparkContext.class)
              .invoke(builder, sc.sc());
            sparksession = builder.getClass().getMethod("getOrCreate").invoke(builder);
          } catch (Exception e) {
            LOG.warn("SparkSession is not supported", e);
            throw e;
          }
        }
      }
    }

    return sparksession;
  }

  @Override
  public SQLContext sqlctx() {
    if (sqlctx == null) {
      synchronized (this) {
        if (sqlctx == null) {
          sqlctx = new SQLContext(sc);
        }
      }
    }
    return sqlctx;
  }

  @Override
  public HiveContext hivectx() {
    if (hivectx == null) {
      synchronized (this) {
        if (hivectx == null) {
          hivectx = new HiveContext(sc.sc());
        }
      }
    }
    return hivectx;
  }


  @Override
  public File getLocalTmpDir() {
    return localTmpDir;
  }

  public synchronized void stop() {
    if (sc != null) {
      sc.stop();
    }
  }

  public void addFile(String path) {
    driver.addFile(path);
  }

  public void addJarOrPyFile(String path) throws Exception {
    driver.addJarOrPyFile(path);
  }
}
