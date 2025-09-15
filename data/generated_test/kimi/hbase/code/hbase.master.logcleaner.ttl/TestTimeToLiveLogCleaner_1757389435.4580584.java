package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, SmallTests.class})
public class TestTimeToLiveLogCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTimeToLiveLogCleaner.class);

  private static final String TTL_CONF_KEY = "hbase.master.logcleaner.ttl";
  private static final long CUSTOM_TTL = 120_000L;

  private TimeToLiveLogCleaner cleaner;
  private EnvironmentEdge injectedEdge;

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    conf.setLong(TTL_CONF_KEY, CUSTOM_TTL);

    cleaner = new TimeToLiveLogCleaner();
    cleaner.setConf(conf);

    injectedEdge = new EnvironmentEdge() {
      @Override
      public long currentTime() {
        return 1_000_000_000L;
      }
    };
    EnvironmentEdgeManager.injectEdge(injectedEdge);
  }

  @After
  public void tearDown() {
    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testFileWithinTtlNotDeleted() throws Exception {
    // 1. Configuration is already set in setUp().
    // 2. Prepare the test conditions: file age = TTL - 1 ms.
    long now = injectedEdge.currentTime();
    long fileTime = now - (CUSTOM_TTL - 1);
    FileStatus mockStatus = new FileStatus(
        1024L, false, 1, 1024L, fileTime,
        new Path("hdfs://test/oldWALs/test-wal.12345"));

    // 3. Test code.
    boolean deletable = cleaner.isFileDeletable(mockStatus);

    // 4. Code after testing.
    assertFalse("File within TTL should not be deleted", deletable);
  }

  @Test
  public void testFileBeyondTtlIsDeleted() throws Exception {
    // 1. Configuration is already set in setUp().
    // 2. Prepare the test conditions: file age = TTL + 1 ms.
    long now = injectedEdge.currentTime();
    long fileTime = now - (CUSTOM_TTL + 1);
    FileStatus mockStatus = new FileStatus(
        1024L, false, 1, 1024L, fileTime,
        new Path("hdfs://test/oldWALs/test-wal.12345"));

    // 3. Test code.
    boolean deletable = cleaner.isFileDeletable(mockStatus);

    // 4. Code after testing.
    assertTrue("File beyond TTL should be deleted", deletable);
  }

  @Test
  public void testFileWithFutureTimestampIsNotDeleted() throws Exception {
    // 1. Configuration is already set in setUp().
    // 2. Prepare the test conditions: file modification time is in the future.
    long now = injectedEdge.currentTime();
    long fileTime = now + 10_000L; // future timestamp
    FileStatus mockStatus = new FileStatus(
        1024L, false, 1, 1024L, fileTime,
        new Path("hdfs://test/oldWALs/test-wal.12345"));

    // 3. Test code.
    boolean deletable = cleaner.isFileDeletable(mockStatus);

    // 4. Code after testing.
    assertFalse("File with future timestamp should not be deleted", deletable);
  }

  @Test
  public void testInvalidWalFilenameIsDeletable() throws Exception {
    // 1. Configuration is already set in setUp().
    // 2. Prepare the test conditions: invalid WAL filename.
    long now = injectedEdge.currentTime();
    long fileTime = now - (CUSTOM_TTL - 1);
    FileStatus mockStatus = new FileStatus(
        1024L, false, 1, 1024L, fileTime,
        new Path("hdfs://test/oldWALs/invalid-filename"));

    // 3. Test code.
    boolean deletable = cleaner.isFileDeletable(mockStatus);

    // 4. Code after testing.
    assertTrue("Invalid WAL filename should be deletable", deletable);
  }
}