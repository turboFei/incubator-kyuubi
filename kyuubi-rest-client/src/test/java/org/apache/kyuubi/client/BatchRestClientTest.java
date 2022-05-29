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

package org.apache.kyuubi.client;

import static org.apache.kyuubi.client.RestClientTestUtil.*;
import static org.junit.Assert.assertEquals;

import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.api.v1.dto.BatchRequest;
import org.apache.kyuubi.client.api.v1.dto.GetBatchesResponse;
import org.apache.kyuubi.client.api.v1.dto.OperationLog;
import org.apache.kyuubi.client.exception.KyuubiRestException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BatchRestClientTest {

  private KyuubiRestClient spnegoClient;
  private KyuubiRestClient basicClient;
  private BatchRestApi spnegoBatchRestApi;
  private BatchRestApi basicBatchRestApi;

  private KerberizedTestHelper kerberizedTestHelper;
  private ServerTestHelper serverTestHelper;

  @Before
  public void setUp() throws Exception {
    kerberizedTestHelper = new KerberizedTestHelper();
    serverTestHelper = new ServerTestHelper();

    kerberizedTestHelper.setup();
    serverTestHelper.setup(BatchTestServlet.class);

    kerberizedTestHelper.login();
    spnegoClient =
        new KyuubiRestClient.Builder("https://localhost:8443")
            .authSchema(KyuubiRestClient.AuthSchema.SPNEGO)
            .build();
    spnegoBatchRestApi = new BatchRestApi(spnegoClient);

    basicClient =
        new KyuubiRestClient.Builder("https://localhost:8443")
            .authSchema(KyuubiRestClient.AuthSchema.BASIC)
            .username(TEST_USERNAME)
            .password(TEST_PASSWORD)
            .build();
    basicBatchRestApi = new BatchRestApi(basicClient);
  }

  @After
  public void tearDown() throws Exception {
    kerberizedTestHelper.stop();
    serverTestHelper.stop();
    spnegoClient.close();
    basicClient.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyHostUrl() {
    KyuubiRestClient basicClient =
        new KyuubiRestClient.Builder("")
            .authSchema(KyuubiRestClient.AuthSchema.BASIC)
            .username("test")
            .password("test")
            .build();
  }

  @Test(expected = KyuubiRestException.class)
  public void testInvalidUrl() throws KyuubiRestException {
    KyuubiRestClient basicClient =
        new KyuubiRestClient.Builder("https://localhost:8443")
            .authSchema(KyuubiRestClient.AuthSchema.BASIC)
            .username("test")
            .password("test")
            .build();
    BatchRestApi invalidBasicBatchRestApi = new BatchRestApi(basicClient);

    invalidBasicBatchRestApi.getBatchById("fake");
  }

  @Test
  public void testNoPasswordBasicClient() throws KyuubiRestException {
    BatchTestServlet.setAuthSchema(BASIC_AUTH);
    BatchTestServlet.allowAnonymous(true);

    KyuubiRestClient noPasswordBasicClient =
        new KyuubiRestClient.Builder("https://localhost:8443")
            .authSchema(KyuubiRestClient.AuthSchema.BASIC)
            .username(TEST_USERNAME)
            .build();
    BatchRestApi noPasswordBasicBatchRestApi = new BatchRestApi(noPasswordBasicClient);

    BatchRequest batchRequest = generateTestBatchRequest();
    Batch expectedBatch = generateTestBatch();
    Batch result = noPasswordBasicBatchRestApi.createBatch(batchRequest);

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());
  }

  @Test
  public void testAnonymousBasicClient() throws KyuubiRestException {
    BatchTestServlet.setAuthSchema(BASIC_AUTH);
    BatchTestServlet.allowAnonymous(true);

    KyuubiRestClient anonymousBasicClient =
        new KyuubiRestClient.Builder("https://localhost:8443")
            .authSchema(KyuubiRestClient.AuthSchema.BASIC)
            .build();
    BatchRestApi anonymousBasicBatchRestApi = new BatchRestApi(anonymousBasicClient);

    BatchRequest batchRequest = generateTestBatchRequest();
    Batch expectedBatch = generateTestBatch();
    Batch result = anonymousBasicBatchRestApi.createBatch(batchRequest);

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());
  }

  @Test
  public void createBatchTest() throws KyuubiRestException {
    // test spnego auth
    BatchTestServlet.setAuthSchema(NEGOTIATE_AUTH);

    BatchRequest batchRequest = generateTestBatchRequest();
    Batch expectedBatch = generateTestBatch();
    Batch result = spnegoBatchRestApi.createBatch(batchRequest);

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());

    // test basic auth
    BatchTestServlet.setAuthSchema(BASIC_AUTH);
    BatchTestServlet.allowAnonymous(false);
    result = basicBatchRestApi.createBatch(batchRequest);

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());
  }

  @Test
  public void getBatchByIdTest() throws KyuubiRestException {
    // test spnego auth
    BatchTestServlet.setAuthSchema(NEGOTIATE_AUTH);

    Batch expectedBatch = generateTestBatch();
    Batch result = spnegoBatchRestApi.getBatchById("71535");

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());

    // test basic auth
    BatchTestServlet.setAuthSchema(BASIC_AUTH);
    BatchTestServlet.allowAnonymous(false);

    result = basicBatchRestApi.getBatchById("71535");

    assertEquals(result.getId(), expectedBatch.getId());
    assertEquals(result.getBatchType(), expectedBatch.getBatchType());
    assertEquals(result.getState(), expectedBatch.getState());
  }

  @Test
  public void getBatchInfoListTest() throws KyuubiRestException {
    // test spnego auth
    BatchTestServlet.setAuthSchema(NEGOTIATE_AUTH);

    GetBatchesResponse expectedBatchesInfo = generateTestBatchesResponse();
    GetBatchesResponse result = spnegoBatchRestApi.listBatches("spark", 0, 10);

    assertEquals(expectedBatchesInfo.getBatches().size(), result.getBatches().size());
    assertEquals(expectedBatchesInfo.getFrom(), result.getFrom());
    assertEquals(expectedBatchesInfo.getTotal(), result.getTotal());

    // test basic auth
    BatchTestServlet.setAuthSchema(BASIC_AUTH);
    BatchTestServlet.allowAnonymous(false);

    result = basicBatchRestApi.listBatches("spark", 0, 10);

    assertEquals(expectedBatchesInfo.getBatches().size(), result.getBatches().size());
    assertEquals(expectedBatchesInfo.getFrom(), result.getFrom());
    assertEquals(expectedBatchesInfo.getTotal(), result.getTotal());
  }

  @Test
  public void getOperationLogTest() throws KyuubiRestException {
    // test spnego auth
    BatchTestServlet.setAuthSchema(NEGOTIATE_AUTH);

    OperationLog expectedOperationLog = generateTestOperationLog();
    OperationLog result = spnegoBatchRestApi.getBatchLocalLog("71535", 0, 2);

    assertEquals(expectedOperationLog.getRowCount(), result.getRowCount());

    // test basic auth
    BatchTestServlet.setAuthSchema(BASIC_AUTH);
    BatchTestServlet.allowAnonymous(false);

    result = basicBatchRestApi.getBatchLocalLog("71535", 0, 2);

    assertEquals(expectedOperationLog.getRowCount(), result.getRowCount());
  }

  @Test
  public void deleteBatchTest() throws KyuubiRestException {
    // test spnego auth
    BatchTestServlet.setAuthSchema(NEGOTIATE_AUTH);
    spnegoBatchRestApi.deleteBatch("71535", true, "b_test");

    // test basic auth
    BatchTestServlet.setAuthSchema(BASIC_AUTH);
    BatchTestServlet.allowAnonymous(false);
    basicBatchRestApi.deleteBatch("71535", true, "b_test");
  }
}
