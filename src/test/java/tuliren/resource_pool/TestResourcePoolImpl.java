package tuliren.resource_pool;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tuliren.resource_pool.exceptions.NoAvailableResourceException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestResourcePoolImpl extends BaseTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(TestResourcePoolImpl.class);

  private static Callable<MockResource> resourceConstructor = MockResource::new;
  private ExecutorService executorService;
  private ResourcePoolImpl<MockResource> dbPoolManager;
  private int maxConnections;
  private int coreConnections;
  private Duration waitingTimeout;
  private Duration keepAliveTime;

  @Before
  public void prepare() throws Exception {
    this.maxConnections = ResourcePoolImpl.DEFAULT_MAX_POOL_SIZE;
    this.coreConnections = ResourcePoolImpl.DEFAULT_CORE_POOL_SIZE;
    this.waitingTimeout = ResourcePoolImpl.DEFAULT_WAITING_TIMEOUT;
    this.keepAliveTime = ResourcePoolImpl.DEFAULT_KEEP_ALIVE_TIME;
  }

  @After
  public void cleanup() throws Exception {
    this.dbPoolManager = null;
    this.executorService = null;
  }

  private void initializeResourcePoolImpl() {
    dbPoolManager = new ResourcePoolImpl<>(resourceConstructor, coreConnections, maxConnections, waitingTimeout, keepAliveTime);
  }

  @Test
  public void testMaxTotalConnections() throws Exception {
    maxConnections = 5;
    waitingTimeout = Duration.ZERO;
    initializeResourcePoolImpl();
    getAllConnections();
    assertIdleConnections(0);
    assertAllocatedConnections(maxConnections);

    try {
      dbPoolManager.getResource(System.currentTimeMillis());
      fail();
    } catch (NoAvailableResourceException e) {
      // expected
    }
  }

  @Test
  public void testMinIdleConnections() throws Exception {
    maxConnections = 10;
    coreConnections = 2;
    keepAliveTime = Duration.millis(2);
    initializeResourcePoolImpl();
    getAndReturnAllConnections();

    // wait for eviction
    sleepSeconds(1);

    assertIdleConnections(coreConnections);
  }

  @Test(timeout = 10 * 1000L) // 10s
  public void testKeepAliveTime() throws Exception {
    maxConnections = 15;
    coreConnections = 5;
    keepAliveTime = Duration.standardSeconds(3);
    initializeResourcePoolImpl();
    getAndReturnAllConnections();

    // all idle connections should be available
    assertIdleConnections(maxConnections);
    assertEvictionTime((int)keepAliveTime.getMillis() / 1000, 2);
  }

  @Test(timeout = 10 * 1000L) // 10s
  public void testMaxWaitTime() throws Exception {
    maxConnections = 1;
    waitingTimeout = Duration.standardSeconds(8);
    initializeResourcePoolImpl();

    executorService = Executors.newFixedThreadPool(2);

    /*
     * 1. first thread gets the only connection, and takes firstThreadProcessTime seconds to return the connection
     */
    int firstThreadProcessTime = 3;
    Future future1 = startFirstThread(firstThreadProcessTime);

    /*
     * 2. second thread starts after secondThreadStartDelay seconds, and gets the connection after waiting for
     *    about firstThreadProcessTime - secondThreadStartDelay seconds
     */
    int secondThreadStartDelay = 1;
    sleepSeconds(secondThreadStartDelay);

    int expectedWaitSeconds = firstThreadProcessTime - secondThreadStartDelay;

    Future future2 = executorService.submit(() -> {
      LOG.info("Second task started");

      // no connection at first
      assertIdleConnections(0);

      // wait for connection
      long startTime = System.currentTimeMillis();
      dbPoolManager.getResource(startTime);

      // check wait time
      int waitSeconds = getSecondsSince(startTime);
      LOG.info("Second task DB waited for connection: {} seconds", waitSeconds);
      assertRoughEqual(waitSeconds, expectedWaitSeconds, 2);

      LOG.info("Second task completed");
    });

    future1.get();
    future2.get();

    executorService.shutdownNow();
  }

  @Test(timeout = 10 * 1000L) // 10s
  public void testNoAvailableConnectionAfterWait() throws Exception {
    maxConnections = 1;
    waitingTimeout = Duration.standardSeconds(1);
    initializeResourcePoolImpl();

    executorService = Executors.newFixedThreadPool(2);

    /*
     * 1. first thread gets the only connection, and takes firstThreadProcessTime seconds to return the connection
     */
    int firstThreadProcessTime = 3;
    Future future1 = startFirstThread(firstThreadProcessTime);

    /*
     * 2. second thread starts after secondThreadStartDelay seconds, and gets the connection after waiting for
     *    about firstThreadProcessTime - secondThreadStartDelay seconds
     */
    int secondThreadStartDelay = 1;
    sleepSeconds(secondThreadStartDelay);

    Future future2 = executorService.submit(() -> {
      LOG.info("Second task started");

      // no connection at first
      assertEquals(0, dbPoolManager.getIdleResources());

      // wait for connection
      long startTime = System.currentTimeMillis();
      try {
        dbPoolManager.getResource(startTime);
        fail();
      } catch (NoAvailableResourceException e) {
        // check exception is thrown after wait time
        int waitSeconds = getSecondsSince(startTime);
        LOG.info("Second task DB waited for connection: {} seconds", waitSeconds);
        assertRoughEqual(waitSeconds, waitingTimeout.getMillis() / 1000, 1);
      }

      LOG.info("Second task completed");
    });

    future1.get();
    future2.get();

    executorService.shutdownNow();
  }

  private void getAllConnections() {
    for (int i = 0; i < maxConnections; ++i) {
      dbPoolManager.getResource(0L);
    }
  }

  private void getAndReturnAllConnections() {
    List<MockResource> connections = Lists.newLinkedList();
    for (int i = 0; i < maxConnections; ++i) {
      connections.add(dbPoolManager.getResource(0L));
    }
    assertIdleConnections(0);
    assertAllocatedConnections(maxConnections);

    for (MockResource connection : connections) {
      dbPoolManager.returnResource(connection);
    }
  }

  private int getSecondsSince(long startTime) {
    return Duration.millis(System.currentTimeMillis() - startTime).toStandardSeconds().getSeconds();
  }

  private void assertIdleConnections(int idleConnections) {
    assertEquals(idleConnections, dbPoolManager.getIdleResources());
  }

  private void assertAllocatedConnections(int activeConnections) {
    assertEquals(activeConnections, dbPoolManager.getAllocatedResources());
  }

  private void assertRoughEqual(long value, long expected, long error) {
    assertTrue(value <= (expected + error) && value >= (expected - error));
  }

  private void assertInRange(long value, long expectedMin, long expectedMax) {
    assertTrue(value >= expectedMin && value <= expectedMax);
  }

  private void assertEvictionTime(int expectedSeconds, int error) {
    long startTime = System.currentTimeMillis();
    while (true) {
      LOG.info("Active connections: {}", dbPoolManager.getIdleResources());
      if (dbPoolManager.getIdleResources() == coreConnections) {
        int waitSeconds = getSecondsSince(startTime);
        LOG.info("Idle connections are evicted after {} seconds", waitSeconds);
        // allow two seconds delay
        assertInRange(waitSeconds, expectedSeconds, expectedSeconds + error);
        break;
      } else {
        sleepSeconds(1);
      }
    }
  }

  private Future startFirstThread(int threadProcessTime) {
    return executorService.submit(() -> {
      LOG.info("First task started");

      // get connection
      long startTime = System.currentTimeMillis();
      MockResource connections = dbPoolManager.getResource(startTime);
      assertIdleConnections(0);

      // return connection after sleepSeconds
      sleepSeconds(threadProcessTime);
      dbPoolManager.returnResource(connections);

      // check connection use time
      int returnTime = getSecondsSince(startTime);
      LOG.info("First task returned DB connection in {} seconds", returnTime);
      assertTrue(returnTime >= threadProcessTime);

      LOG.info("First task completed");
    });
  }

}
