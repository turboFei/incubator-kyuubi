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

package org.apache.hive.beeline;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Driver;
import java.util.*;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

public class KyuubiBeeLine extends BeeLine {
  public static final String KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER =
      "org.apache.kyuubi.jdbc.KyuubiHiveDriver";
  private static final ResourceBundle beelineResourceBundle =
      ResourceBundle.getBundle(BeeLine.class.getSimpleName());
  protected KyuubiCommands commands = new KyuubiCommands(this);
  private Driver defaultDriver = null;
  public static final String SPARK_SUBMIT_COMMAND_PREFIX = "sparksubmit";
  public static final String SCALA_COMMAND_PREFIX = "scala";
  ResourceBundle kyuubiResourceBundle = new KyuubiBeelineResourceBundle();

  public KyuubiBeeLine() {
    this(true);
  }

  public KyuubiBeeLine(boolean isBeeLine) {
    super(isBeeLine);
    try {
      Field commandsField = BeeLine.class.getDeclaredField("commands");
      commandsField.setAccessible(true);
      commandsField.set(this, commands);

      Field resourceBundleField = BeeLine.class.getDeclaredField("resourceBundle");
      resourceBundleField.setAccessible(true);
      Field modifiers = Field.class.getDeclaredField("modifiers");
      modifiers.setAccessible(true);
      modifiers.setInt(resourceBundleField, resourceBundleField.getModifiers() & ~Modifier.FINAL);
      resourceBundleField.set(null, kyuubiResourceBundle);

      ReflectiveCommandHandler sparkSubmitHandler =
          new ReflectiveCommandHandler(
              this,
              new String[] {"sparksubmit"},
              new Completer[] {new StringsCompleter(getSparkSubmitExample())});
      ReflectiveCommandHandler scalaHandler =
          new ReflectiveCommandHandler(
              this,
              new String[] {"scala"},
              new Completer[] {new StringsCompleter("!scala <code>")});

      Field commandHandlersField = BeeLine.class.getDeclaredField("commandHandlers");
      commandHandlersField.setAccessible(true);
      List<CommandHandler> commandHandlers = new ArrayList<CommandHandler>();
      commandHandlers.addAll(Arrays.asList((CommandHandler[]) commandHandlersField.get(this)));
      commandHandlers.add(sparkSubmitHandler);
      commandHandlers.add(scalaHandler);
      commandHandlersField.set(
          this, commandHandlers.toArray(new CommandHandler[commandHandlers.size()]));
    } catch (Throwable t) {
      t.printStackTrace();
      throw new ExceptionInInitializerError("Failed to inject kyuubi commands");
    }
    try {
      defaultDriver =
          (Driver)
              Class.forName(
                      KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER,
                      true,
                      Thread.currentThread().getContextClassLoader())
                  .newInstance();
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(KYUUBI_BEELINE_DEFAULT_JDBC_DRIVER + "-missing");
    }
  }

  /** Starts the program. */
  public static void main(String[] args) throws IOException {
    mainWithInputRedirection(args, null);
  }

  /**
   * Starts the program with redirected input. For redirected output, setOutputStream() and
   * setErrorStream can be used. Exits with 0 on success, 1 on invalid arguments, and 2 on any other
   * error
   *
   * @param args same as main()
   * @param inputStream redirected input, or null to use standard input
   */
  public static void mainWithInputRedirection(String[] args, InputStream inputStream)
      throws IOException {
    KyuubiBeeLine beeLine = new KyuubiBeeLine();
    try {
      int status = beeLine.begin(args, inputStream);

      if (!Boolean.getBoolean(BeeLineOpts.PROPERTY_NAME_EXIT)) {
        System.exit(status);
      }
    } finally {
      beeLine.close();
    }
  }

  protected Driver getDefaultDriver() {
    return defaultDriver;
  }

  @Override
  String getApplicationTitle() {
    Package pack = BeeLine.class.getPackage();

    return loc(
        "app-introduction",
        new Object[] {
          "Beeline",
          pack.getImplementationVersion() == null ? "???" : pack.getImplementationVersion(),
          "Apache Kyuubi (Incubating)",
        });
  }

  public String getSparkSubmitExample() {
    return "{\n"
        + "  \"mainClass\": \"org.apache.spark.examples.SparkPi\",\n"
        + "  \"conf\": {\n"
        + "    \"spark.driver.memory\": \"1g\"\n"
        + "  },\n"
        + "  \"resource\": \"viewfs://apollo-rno/user/b_stf/spark-examples_2.12-3.1.1.0.5.0.jar\",\n"
        + "  \"args\": [\"1\" ],\n"
        + "  \"returnOnSubmitted\": \"true\"\n"
        + "}\n";
  }

  static class KyuubiBeelineResourceBundle extends ListResourceBundle {
    private Object[][] contents = new Object[beelineResourceBundle.keySet().size() + 2][];

    public KyuubiBeelineResourceBundle() {
      int i = 0;
      for (String key : beelineResourceBundle.keySet()) {
        contents[i] = new Object[] {key, beelineResourceBundle.getString(key)};
        i++;
      }
      contents[i] = new Object[] {"help-scala", "Scala code"};
      contents[i + 1] = new Object[] {"help-sparksubmit", "Spark submit command"};
    }

    @Override
    protected Object[][] getContents() {
      return contents;
    }
  }
}
