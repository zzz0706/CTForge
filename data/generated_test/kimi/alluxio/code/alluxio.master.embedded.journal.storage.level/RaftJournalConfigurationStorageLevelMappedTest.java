package alluxio.master.journal.raft;

import static org.junit.Assert.assertEquals;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RaftJournalConfigurationStorageLevelMappedTest {

  @Before
  public void setUp() {
    ServerConfiguration.reset();
  }

  @After
  public void tearDown() {
    ServerConfiguration.reset();
  }

  @Test
  public void storageLevelMappedIsAccepted() {
    // 1. Prepare the test conditions by injecting the desired value into ServerConfiguration
    ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_STORAGE_LEVEL, "MAPPED");

    // 2. Invoke the method under test
    RaftJournalConfiguration actualConfig =
        RaftJournalConfiguration.defaults(NetworkAddressUtils.ServiceType.MASTER_RAFT);

    // 3. Compute expected value dynamically
    RaftJournalConfiguration.StorageLevel expectedLevel =
        RaftJournalConfiguration.StorageLevel.MAPPED;

    // 4. Assert the result
    assertEquals(expectedLevel, actualConfig.getStorageLevel());
  }
}