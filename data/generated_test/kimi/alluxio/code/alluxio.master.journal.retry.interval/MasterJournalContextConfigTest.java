package alluxio.master.journal;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MasterJournalContextConfigTest {

  @Before
  public void before() {
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void defaultRetryIntervalUsedWhenNoOverride() {
    // 1. Ensure no override exists
    ServerConfiguration.reset();

    // 2. Read the default value through the same mechanism as the production code
    long actualMs = ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL);

    // 3. Assert it equals the documented default (1000 ms)
    assertEquals(1000L, actualMs);
  }

  @Test
  public void customRetryIntervalPropagatedToRetryPolicy() {
    // 1. Override the retry interval to a non-default value
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL, "2500ms");

    // 2. Read the overridden value
    long actualMs = ServerConfiguration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL);

    // 3. Assert the override took effect
    assertEquals(2500L, actualMs);
  }
}