package net.penguinsan.mutex;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.UUID;

public class MutexRedisService implements MutexService {

  private final JedisPool jedisPool;
  private final long lockTimeSec;
  private final long timeWaitMillis;

  public MutexRedisService(
          JedisPool jedisPool,
          long lockTimeSec,
          long timeWaitMillis) {
    this.jedisPool = jedisPool;
    this.lockTimeSec = lockTimeSec;
    this.timeWaitMillis = timeWaitMillis;
  }

  protected JedisPool getJedisPool() {
    return this.jedisPool;
  }

  @Override
  public Mutex waitForSingleObject(String objectKey) {
    final long startTime = System.currentTimeMillis();
    final String lockId = UUID.randomUUID().toString();
    while(true) {
      try (Jedis r = this.jedisPool.getResource()) {
        String result = r.set(
                objectKey,
                lockId,
                SetParams.setParams().nx().ex(this.lockTimeSec));
        if ("OK".equalsIgnoreCase(result)) {
          break;
        }
      }
      if (System.currentTimeMillis() - startTime > this.timeWaitMillis) {
        throw new MutexTimeoutException("MUTEX wait timeout : mutex=" + objectKey);
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
      }
    }
    System.out.printf("create key : lock_id=%s\n", lockId);
    return new Mutex(objectKey, lockId, this::releaseMutex);
  }

  @Override
  public void releaseMutex(Mutex m) {
    final String objectKey = m.getObjectKey();
    try (Jedis r = this.jedisPool.getResource()) {
      r.del(objectKey);
    }
    System.out.printf("delete key : lock_id=%s\n", m.getLockId());
  }
}
