package net.penguinsan.mutex;

import java.util.function.Consumer;

public class Mutex implements AutoCloseable {

  private final String objectKey;
  private final String lockId;
  private final Consumer<Mutex> releaseFunc;

  Mutex(String objectKey, String lockId, Consumer<Mutex> releaseFunc) {
    this.objectKey = objectKey;
    this.lockId = lockId;
    this.releaseFunc = releaseFunc;
  }

  public String getObjectKey() {
    return this.objectKey;
  }

  public String getLockId() {
    return this.lockId;
  }

  @Override
  public void close() {
    this.releaseFunc.accept(this);
  }
}
