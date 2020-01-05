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

import com.github.ljishen.IRocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * RocksDB binding for <a href="http://rocksdb.org/">RocksDB</a>.
 *
 * <p>See {@code rocksdb/README.md} for details.
 */
public class RocksDBClient extends DB {

  static final String PROPERTY_ROCKSDB_DIR = "rocksdb.dir";
  static final String PROPERTY_ROCKSDB_OPTIONS_FILE = "rocksdb.optionsfile";
  static final String PROPERTY_ROCKSDB_REGISTRY_PORT = "rocksdb.registryport";
  static final String PROPERTY_ROCKSDB_REGISTRY_HOST = "rocksdb.registryhost";

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBClient.class);

  private static IRocksDB iRocksDb;

  private static AtomicInteger references = new AtomicInteger();

  @Override
  public void init() throws DBException {
    synchronized (RocksDBClient.class) {
      if (iRocksDb == null) {
        try {
          iRocksDb = initRocksDB();
        } catch (final RemoteException | NotBoundException e) {
          throw new DBException(e);
        }
      }
    }

    references.incrementAndGet();
  }

  /**
   * Initializes and opens the RocksDB database.
   *
   * <p>Should only be called with a {@code synchronized(RocksDBClient.class)` block}.
   *
   * @return The initialized and open IRocksDB instance.
   */
  private IRocksDB initRocksDB() throws
      RemoteException, NotBoundException {

    int registryPort = Integer.valueOf(
        getProperties().getProperty(
            PROPERTY_ROCKSDB_REGISTRY_PORT, Integer.toString(Registry.REGISTRY_PORT)));

    Registry registry = LocateRegistry.getRegistry(getProperties().getProperty(
        PROPERTY_ROCKSDB_REGISTRY_HOST, "localhost"), registryPort);
    IRocksDB db = (IRocksDB) registry.lookup("RocksDB-" + registryPort);

    db.open(
        getProperties().getProperty(PROPERTY_ROCKSDB_DIR),
        getProperties().getProperty(PROPERTY_ROCKSDB_OPTIONS_FILE));

    return db;
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();

    // Only the last DB instance can reap the resources since other instances
    // may still be using them. That's also why we need most of the member variables
    // to be static.
    if (references.decrementAndGet() != 0) {
      return;
    }

    try {
      iRocksDb.close();
      iRocksDb = null;
    } catch (RemoteException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DBException(e);
    }
  }

  @Override
  public Status read(
      final String table,
      final String key,
      final Set<String> fields,
      final Map<String, ByteIterator> result) {

    try {
      final byte[] values = iRocksDb.get(table, key);
      if (values == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(values, fields, result);
      return Status.OK;
    } catch (final RemoteException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      final String table,
      final String startkey,
      final int recordCount,
      final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {

    try {
      List<byte[]> rawValues = iRocksDb.bulkGet(table, startkey, recordCount);
      for (byte[] value : rawValues) {
        final HashMap<String, ByteIterator> values = new HashMap<>();
        deserializeValues(value, fields, values);
        result.add(values);
      }

      return Status.OK;
    } catch (final RemoteException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(
      final String table, final String key, final Map<String, ByteIterator> values) {
    // TODO(AR) consider if this would be faster with merge operator

    try {
      final byte[] currentValues = iRocksDb.get(table, key);
      if (currentValues == null) {
        return Status.NOT_FOUND;
      }

      final Map<String, ByteIterator> result = new HashMap<>();
      deserializeValues(currentValues, null, result);

      // update
      result.putAll(values);

      // store
      iRocksDb.put(table, key, serializeValues(result));

      return Status.OK;

    } catch (final IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(
      final String table, final String key, final Map<String, ByteIterator> values) {

    try {
      iRocksDb.put(table, key, serializeValues(values));

      return Status.OK;
    } catch (final IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      iRocksDb.delete(table, key);

      return Status.OK;
    } catch (final RemoteException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  private Map<String, ByteIterator> deserializeValues(
      final byte[] values, final Set<String> fields, final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while (offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

  Set<String> getColumnFamilyNames() throws RemoteException {
    return iRocksDb.getColumnFamilyNames();
  }
}