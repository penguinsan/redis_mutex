package net.penguinsan.mutex;

public class MutexTimeoutException extends RuntimeException {
  MutexTimeoutException(String message) {
    super(message);
  }
}
