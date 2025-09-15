package alluxio.master.journal.raft;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DefaultStorageLevelIsDiskTest {

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    ServerConfiguration.reset();
  }

  @Test
  public void DefaultStorageLevelIsDisk() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    String expected = "DISK";

    // 2. Prepare the test conditions.
    ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_STORAGE_LEVEL, expected);

    // 3. Test code.
    String actual = ServerConfiguration.get(PropertyKey.MASTER_EMBEDDED_JOURNAL_STORAGE_LEVEL);

    // 4. Code after testing.
    assertEquals(expected, actual);
  }
}