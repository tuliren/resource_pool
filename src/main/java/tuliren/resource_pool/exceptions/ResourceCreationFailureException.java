package tuliren.resource_pool.exceptions;

public class ResourceCreationFailureException extends ResourcePoolException {
  public ResourceCreationFailureException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
