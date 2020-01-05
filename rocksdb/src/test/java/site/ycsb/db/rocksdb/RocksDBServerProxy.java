package site.ycsb.db.rocksdb;

import com.github.ljishen.RocksDBServer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksDBServerProxy {
  private static AtomicInteger counter = new AtomicInteger();
  private static String registryPort;

  public static String getRegistryPort() {
    return registryPort;
  }

  private static String getNextRegistryPort() {
    registryPort = String.valueOf(1099 + counter.getAndIncrement());
    return registryPort;
  }

  public static Process start() throws IOException, InterruptedException {
    Process server = new ProcessBuilder(
        System.getProperty("java.home") + "/bin/java",
        "-classpath",
        System.getProperty("java.class.path"),
        RocksDBServer.class.getName(),
        getNextRegistryPort()
    ).inheritIO().start();

    // wait for the process to be ready
    Thread.sleep(1000);

    return server;
  }
}