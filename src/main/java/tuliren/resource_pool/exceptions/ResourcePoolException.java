package tuliren.resource_pool.exceptions;

public class ResourcePoolException extends RuntimeException {

  public ResourcePoolException() {
    super();
  }

  public ResourcePoolException(String message) {
    super(message);
  }

  public ResourcePoolException(Throwable throwable) {
    super(throwable);
  }

  public ResourcePoolException(String message, Throwable throwable) {
    super(message, throwable);
  }

}
