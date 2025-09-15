package alluxio.master.journal.raft;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.raft.RaftJournalConfiguration.StorageLevel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class RaftJournalStorageLevelValidationTest {

  private static final PropertyKey KEY =
      PropertyKey.MASTER_EMBEDDED_JOURNAL_STORAGE_LEVEL;

  @Before
  public void before() {
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void storageLevelIsValid() {
    String level = ServerConfiguration.get(KEY);
    boolean valid = false;
    for (StorageLevel sl : StorageLevel.values()) {
      if (sl.name().equalsIgnoreCase(level)) {
        valid = true;
        break;
      }
    }
    assertTrue("Invalid value for " + KEY + ": " + level, valid);
  }
}