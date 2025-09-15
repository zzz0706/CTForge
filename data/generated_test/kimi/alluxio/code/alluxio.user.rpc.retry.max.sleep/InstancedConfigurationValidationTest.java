package alluxio.conf;

import alluxio.Constants;
import alluxio.util.ConfigurationUtils;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class InstancedConfigurationValidationTest {

  private static final Logger LOGGER = Logger.getLogger(InstancedConfiguration.class);

  private TestAppender appender;

  @Before
  public void setUp() {
    appender = new TestAppender();
    LOGGER.addAppender(appender);
    LOGGER.setLevel(Level.WARN);
  }

  @After
  public void tearDown() {
    LOGGER.removeAppender(appender);
  }

  @Test
  public void verifyValidationWarningWhenConnectWaitTimeShorterThanRetryMaxSleep() {
    // 1. Create a fresh configuration
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Override MASTER_WORKER_CONNECT_WAIT_TIME to 1 s (1000 ms) while keeping
    //    USER_RPC_RETRY_MAX_SLEEP_MS at its default 3 s (3000 ms)
    conf.set(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME, "1s");

    // 3. Trigger validation
    conf.validate();

    // 4. Assert that the expected warning is logged
    boolean warningFound = appender.events.stream()
        .anyMatch(e -> e.getRenderedMessage().contains(
            PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME.getName()) &&
            e.getRenderedMessage().contains(
                PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS.getName()) &&
            e.getRenderedMessage().contains("is smaller than"));

    assertTrue("Expected warning about MASTER_WORKER_CONNECT_WAIT_TIME < USER_RPC_RETRY_MAX_SLEEP_MS",
        warningFound);
  }

  private static class TestAppender extends AppenderSkeleton {
    private final List<LoggingEvent> events = new ArrayList<>();

    @Override
    protected void append(LoggingEvent event) {
      events.add(event);
    }

    @Override
    public void close() {
      // no-op
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }
  }
}