package alluxio.master.journal;

import static org.junit.Assert.assertTrue;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for validating {@link PropertyKey#MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES}.
 */
public class JournalCheckpointPeriodEntriesValidationTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() {
    mConf = new InstancedConfiguration(alluxio.conf.ServerConfiguration.global());
  }

  @After
  public void after() {
    mConf = null;
  }

  @Test
  public void validateCheckpointPeriodEntries() {
    // 1. Obtain the value from configuration without setting it in test code.
    long periodEntries = mConf.getLong(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);

    // 2. Prepare test conditions: none, we only read the value.

    // 3. Test code: validate that the value is a positive integer.
    assertTrue(
        "alluxio.master.journal.checkpoint.period.entries must be a positive integer",
        periodEntries > 0);

    // 4. Code after testing: nothing to do.
  }
}