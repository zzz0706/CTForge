package alluxio.master.journal;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for validating configuration value of
 * {@code alluxio.master.journal.tailer.shutdown.quiet.wait.time}.
 */
public class JournalTailerQuietWaitTimeConfigTest {

  private static final PropertyKey KEY = PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS;

  @Before
  public void before() {
    // Reset the configuration to the default state
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    // Reset the configuration after the test
    ServerConfiguration.reset();
  }

  /**
   * Tests that the configured value for {@code alluxio.master.journal.tailer.shutdown.quiet.wait.time}
   * is a non-negative duration.
   */
  @Test
  public void testQuietWaitTimeIsNonNegative() {
    long quietWaitMs = ServerConfiguration.getMs(KEY);
    assertTrue("alluxio.master.journal.tailer.shutdown.quiet.wait.time must be non-negative",
        quietWaitMs >= 0);
  }
}