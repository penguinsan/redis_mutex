package net.penguinsan.mutex;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Main {

  public static void main(String args[]) {
    final JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxTotal(16);
    final JedisPool pool = new JedisPool(config, "localhost", 6379);
    final long lockTimeSec = 5L; // ロックの最長時間は5秒（5秒経っても能動的にUnlockされない場合は強制的にUnlockされる）
    final long timeWaitMillis = 15000L; // ロック獲得のために待つのは15秒まで。15秒待っても獲得できない場合はExceptionがスローされる
    final MutexService mutexService = new MutexRedisService(pool, lockTimeSec, timeWaitMillis);
    Main main = new Main(mutexService);
    // 並列動作
    List<CompletableFuture<Void>> cfs = new ArrayList<>();
    System.out.println("start(parallel)");
    for (int i = 1; i <= 10; i++) {
      final int n = i;
      cfs.add(CompletableFuture.runAsync(() -> main.parallel(n)));
    }
    CompletableFuture.allOf(cfs.toArray(new CompletableFuture[cfs.size()])).join();
    System.out.println("finish(parallel)");
    // ロックを使った直列動作
    cfs.clear();
    System.out.println("start(serial)");
    for (int i = 1; i <= 10; i++) {
      final int n = i;
      cfs.add(CompletableFuture.runAsync(() -> main.serial(n)));
    }
    CompletableFuture.allOf(cfs.toArray(new CompletableFuture[cfs.size()])).join();
    System.out.println("finish(serial)");
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
    }
  }

  private static final String MUTEX_OBJECT = "key";

  private final MutexService mutexService;

  public Main(MutexService mutexService) {
    this.mutexService = mutexService;
  }

  private void parallel(int n) {
    processImpl(n, 500L);
  }

  private void serial(int n) {
    System.out.printf("wait %d\n", n);
    try (Mutex m = this.mutexService.waitForSingleObject(MUTEX_OBJECT)) {
      System.out.printf("Lock by %d\n", n);
      processImpl(n, 500L);
      System.out.printf("Unlock by %d\n", n);
    }
  }

  private void processImpl(int n, long timeMillis) {
    System.out.printf("process start %d\n", n);
    sleep(timeMillis);
    System.out.printf("process end %d\n", n);
  }
}
