/*
 * Copyright (c) 2018-2020 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.rocksdb;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.CoreWorkload;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class RocksDBClientTest {
  private static final String MOCK_TABLE = "ycsb";
  private static final String MOCK_KEY0 = "0";
  private static final String MOCK_KEY1 = "1";
  private static final String MOCK_KEY2 = "2";
  private static final String MOCK_KEY3 = "3";
  private static final int NUM_RECORDS = 10;
  private static final String FIELD_PREFIX = CoreWorkload.FIELD_NAME_PREFIX_DEFAULT;
  private static final Map<String, ByteIterator> MOCK_DATA;

  private static Process server;

  static {
    MOCK_DATA = new HashMap<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      MOCK_DATA.put(FIELD_PREFIX + i, new StringByteIterator("value" + i));
    }
  }

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private RocksDBClient client;

  @BeforeClass
  public static void startServer() throws IOException, InterruptedException {
    server = RocksDBServerProxy.start();
  }

  @AfterClass
  public static void stopServer() {
    server.destroy();
  }

  @Before
  public void setup() throws DBException {
    final String optionsPath = RocksDBClient.class.getClassLoader()
        .getResource("testcase.ini").getPath();
    final String dbPath = tmpFolder.getRoot().getAbsolutePath();

    final Properties properties = new Properties();
    properties.setProperty(RocksDBClient.PROPERTY_ROCKSDB_DIR, dbPath);
    properties.setProperty(RocksDBClient.PROPERTY_ROCKSDB_OPTIONS_FILE, optionsPath);
    properties.setProperty(RocksDBClient.PROPERTY_ROCKSDB_REGISTRY_PORT,
        RocksDBServerProxy.getRegistryPort());

    client = new RocksDBClient();
    client.setProperties(properties);

    client.init();
  }

  @After
  public void tearDown() throws DBException {
    client.cleanup();
  }

  @Test
  public void insertAndRead() {
    final Status insertResult = client.insert(MOCK_TABLE, MOCK_KEY0, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Set<String> fields = MOCK_DATA.keySet();
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    final Status readResult = client.read(MOCK_TABLE, MOCK_KEY0, fields, resultParam);
    assertEquals(Status.OK, readResult);
  }

  @Test
  public void insertAndDelete() {
    final Status insertResult = client.insert(MOCK_TABLE, MOCK_KEY1, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Status result = client.delete(MOCK_TABLE, MOCK_KEY1);
    assertEquals(Status.OK, result);
  }

  @Test
  public void insertUpdateAndRead() {
    final Map<String, ByteIterator> newValues = new HashMap<>(NUM_RECORDS);

    final Status insertResult = client.insert(MOCK_TABLE, MOCK_KEY2, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    for (int i = 0; i < NUM_RECORDS; i++) {
      newValues.put(FIELD_PREFIX + i, new StringByteIterator("newvalue" + i));
    }

    final Status result = client.update(MOCK_TABLE, MOCK_KEY2, newValues);
    assertEquals(Status.OK, result);

    //validate that the values changed
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    client.read(MOCK_TABLE, MOCK_KEY2, MOCK_DATA.keySet(), resultParam);

    for (int i = 0; i < NUM_RECORDS; i++) {
      assertEquals("newvalue" + i, resultParam.get(FIELD_PREFIX + i).toString());
    }
  }

  @Test
  public void insertAndScan() {
    final Status insertResult = client.insert(MOCK_TABLE, MOCK_KEY3, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Set<String> fields = MOCK_DATA.keySet();
    final Vector<HashMap<String, ByteIterator>> resultParam = new Vector<>(NUM_RECORDS);
    final Status result = client.scan(MOCK_TABLE, MOCK_KEY3, NUM_RECORDS, fields, resultParam);
    assertEquals(Status.OK, result);
  }
}
