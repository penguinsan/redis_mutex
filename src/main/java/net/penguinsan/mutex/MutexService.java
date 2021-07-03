package net.penguinsan.mutex;

public interface MutexService {

  Mutex waitForSingleObject(String objectKey);
  void releaseMutex(Mutex m);

}
