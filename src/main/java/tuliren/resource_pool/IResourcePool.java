package tuliren.resource_pool;

import java.io.Closeable;

import org.joda.time.Duration;

interface IResourcePool<T extends Closeable> extends Closeable {

  T getResource(long timestamp);

  void returnResource(T resource);

  @Override
  void close();

  void close(Duration timeout);

}
