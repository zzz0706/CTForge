package alluxio.master.journal;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MasterJournalContextTest {

  @Test
  public void defaultRetryIntervalUsedWhenNoOverride() {
    // 1. Reset the global configuration to ensure no overrides exist
    ServerConfiguration.reset();

    // 2. Obtain the default retry interval directly from the global ServerConfiguration
    long actualRetryIntervalMs = ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL);

    // 3. Verify the default value is 1000 ms
    long expectedRetryIntervalMs = 1000;
    assertEquals(expectedRetryIntervalMs, actualRetryIntervalMs);
  }
}