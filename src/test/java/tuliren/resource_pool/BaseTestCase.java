package tuliren.resource_pool;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.joda.time.Duration;
import org.junit.Before;

public class BaseTestCase {

  private static final String SEPARATOR = "--------------------";

  public BaseTestCase() {
    Logger rootLogger = Logger.getRootLogger();

    rootLogger.setLevel(Level.ALL);

    ConsoleAppender consoleAppender = new ConsoleAppender(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n"), ConsoleAppender.SYSTEM_ERR);
    consoleAppender.setName("test-console-appender");
    consoleAppender.setFollow(true);

    rootLogger.removeAppender("test-console-appender");
    rootLogger.addAppender(consoleAppender);
  }

  @Before
  public final void printSeparators() throws Exception {
    System.out.println(SEPARATOR);
    System.out.println("TEST: " + getName());
    System.out.println(SEPARATOR);
  }

  private String getName() {
    return getClass().getSimpleName();
  }

  protected void sleepSeconds(int seconds) {
    try {
      Thread.sleep(Duration.standardSeconds(seconds).getMillis());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected void sleepMillis(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
