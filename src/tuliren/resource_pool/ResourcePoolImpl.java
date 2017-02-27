package tuliren.resource_pool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tuliren.resource_pool.exceptions.NoAvailableResourceException;
import tuliren.resource_pool.exceptions.ResourceClosureFailureException;
import tuliren.resource_pool.exceptions.ResourceCreationFailureException;

class ResourcePoolImpl<T extends Closeable> implements IResourcePool<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ResourcePoolImpl.class);
  private static final Duration MAX_IDLE_RESOURCE_CHECK_TIME = Duration.standardSeconds(10);
  private static final Duration AUTO_CLOSE_IDLE_RESOURCE_THRESHOLD = Duration.ZERO;

  private final Callable<T> resourceConstructor;
  private final int corePoolSize;
  private final int maxPoolSize;
  private final Duration waitingTimeout;
  private final Duration keepAliveTime;
  private final ScheduledExecutorService idleResourceTerminator;

  private final Set<T> allocatedResources = Sets.newHashSet();
  private final Queue<T> idleResources = Lists.newLinkedList();
  private final Lock lock = new ReentrantLock(true);
  private final Condition returnResource = lock.newCondition();

  private long lastActiveTimestamp = System.currentTimeMillis();
  private boolean closed = false;

  private ResourcePoolImpl(Callable<T> resourceConstructor, int corePoolSize, int maxPoolSize, Duration waitingTimeout, Duration keepAliveTime) {
    this.resourceConstructor = resourceConstructor;
    this.corePoolSize = corePoolSize;
    this.maxPoolSize = maxPoolSize;
    this.waitingTimeout = waitingTimeout;
    this.keepAliveTime = keepAliveTime;
    if (keepAliveTime.isLongerThan(AUTO_CLOSE_IDLE_RESOURCE_THRESHOLD)) {
      // use daemon thread so that the executor service won't block JVM exit
      this.idleResourceTerminator = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build());
    } else {
      this.idleResourceTerminator = null;
    }
  }

  static <T extends Closeable> ResourcePoolImpl<T> create(Callable<T> dbConstructor, int corePoolSize, int maxPoolSize, Duration waitingTimeout, Duration keepAliveTime) {
    return new ResourcePoolImpl<>(dbConstructor, corePoolSize, maxPoolSize, waitingTimeout, keepAliveTime);
  }

  @Override
  public T getResource(long timestamp) {
    long timeoutThreshold = timestamp + waitingTimeout.getMillis();

    try {
      if (lock.tryLock(waitingTimeout.getMillis(), TimeUnit.MILLISECONDS)) {
        try {
          // check for close before waiting
          checkCloseStatus();

          int totalResources = allocatedResources.size() + idleResources.size();

          // wait for resource to be returned when no resource is available, no new resource can be created and within timeout threshold
          while (idleResources.isEmpty() && totalResources >= maxPoolSize && System.currentTimeMillis() < timeoutThreshold) {
            try {
              returnResource.awaitUntil(new Date(timeoutThreshold));
            } catch (InterruptedException e) {
              LOG.error("Waiting for resource interrupted", e);
              throw new ResourceCreationFailureException("Waiting for resource interrupted", e);
            }
          }

          // check for close after waiting
          checkCloseStatus();

          if (!idleResources.isEmpty()) {
            return getIdleResource();
          }

          // when no resource is available but new resource can be created
          if (totalResources < maxPoolSize) {
            return createNewResource();
          }
        } finally {
          lock.unlock();
        }
      }

      LOG.error("No available resource after waiting for {} seconds", waitingTimeout.getStandardSeconds());
      throw new NoAvailableResourceException("No available resource after waiting for " + waitingTimeout.getStandardSeconds() + " seconds");
    } catch (InterruptedException e) {
      LOG.error("Waiting for resource interrupted", e);
      throw new ResourceCreationFailureException("Waiting for resource interrupted", e);
    }
  }

  private void checkCloseStatus() {
    if (closed) {
      LOG.error("Cannot get resource because resource manager has been closed");
      throw new IllegalStateException("Cannot get resource resource because resource manager has been closed");
    }
  }

  private T getIdleResource() {
    T resource = idleResources.remove();
    allocatedResources.add(resource);
    return resource;
  }

  private T createNewResource() {
    try {
      T newResource = resourceConstructor.call();
      allocatedResources.add(newResource);
      return newResource;
    } catch (Exception e) {
      LOG.error("T resource creation failed", e);
      throw new ResourceCreationFailureException("T resource creation failed", e);
    }
  }

  @Override
  public void returnResource(T resource) {
    lock.lock();
    try {
      if (!allocatedResources.remove(resource)) {
        throw new IllegalArgumentException("Returned resource does not come from this resource manager");
      }
      idleResources.add(resource);
      returnResource.signalAll();
      lastActiveTimestamp = System.currentTimeMillis();
      if (idleResourceTerminator != null) {
        idleResourceTerminator.schedule(checkIdleResource(), keepAliveTime.getMillis(), TimeUnit.MILLISECONDS);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {
    close(Duration.ZERO);
  }

  public void close(Duration timeout) {
    LOG.debug("T manager close started");

    Exception lastException = null;
    int failed = 0;
    if (idleResourceTerminator != null) {
      idleResourceTerminator.shutdownNow();
    }

    lock.lock();
    try {
      closed = true;

      if (timeout.getMillis() > 0 && allocatedResources.size() > 0) {
        try {
          returnResource.await(timeout.getMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOG.warn("Waiting for return of resource to close resource manager interrupted");
        }
      }

      if (allocatedResources.size() > 0) {
        LOG.warn("Closing {} resource that are still in use", allocatedResources.size());
      }

      for (T resource : allocatedResources) {
        try {
          resource.close();
        } catch (Exception e) {
          LOG.error("T closure failed", e);
          ++failed;
          lastException = e;
        }
      }
      allocatedResources.clear();

      for (T resource : idleResources) {
        try {
          resource.close();
        } catch (Exception e) {
          LOG.error("T closure failed", e);
          ++failed;
          lastException = e;
        }
      }
      idleResources.clear();

      if (lastException != null) {
        throw new ResourceClosureFailureException(
            String.format("%d out of %d resource closure(s) failed; last exception: ", failed, allocatedResources.size() + idleResources.size()),
            lastException
        );
      }
    } finally {
      lock.unlock();
    }

    LOG.debug("T manager close completed");
  }

  private Runnable checkIdleResource() {
    return () -> {
      // unsafe initial timestamp and resource check without locking
      if (idleResources.size() <= corePoolSize && System.currentTimeMillis() - lastActiveTimestamp < keepAliveTime.getMillis()) {
        return;
      }

      // more stringent check if initial check passes
      try {
        // abort if the lock cannot be acquired in MAX_IDLE_RESOURCE_CHECK_TIME
        if (lock.tryLock(MAX_IDLE_RESOURCE_CHECK_TIME.getMillis(), TimeUnit.MILLISECONDS)) {
          try {
            while (idleResources.size() > corePoolSize) {
              try {
                idleResources.remove().close();
              } catch (IOException e) {
                LOG.error("Failed to close idle resource", e);
                throw new ResourceClosureFailureException("Failed to close idle resource", e);
              }
            }
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException e) {
        LOG.error("Waiting for lock to close idle resource is interrupted", e);
      }
    };
  }

  @VisibleForTesting
  boolean isClosed() {
    return closed;
  }

  @VisibleForTesting
  int getIdleResources() {
    return idleResources.size();
  }

  @VisibleForTesting
  int getAllocatedResources() {
    return allocatedResources.size();
  }

}
