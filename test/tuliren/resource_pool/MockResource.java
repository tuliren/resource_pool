package tuliren.resource_pool;

import java.io.Closeable;
import java.io.IOException;

public class MockResource implements Closeable {
  @Override
  public void close() throws IOException {
  }
}
