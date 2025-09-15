package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

import static org.junit.Assert.assertFalse;

@Category({MasterTests.class, SmallTests.class})
public class TestTimeToLiveLogCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTimeToLiveLogCleaner.class);

  @Test
  public void testFileWithinTtlNotDeleted() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    long ttl = conf.getLong("hbase.master.logcleaner.ttl", 600000L);

    // 2. Prepare the test conditions.
    long now = 1_000_000_000L;
    long fileTime = now - (ttl - 1); // age = ttl - 1 ms, still within TTL

    EnvironmentEdge edge = new EnvironmentEdge() {
      @Override
      public long currentTime() {
        return now;
      }
    };
    EnvironmentEdgeManager.injectEdge(edge);

    TimeToLiveLogCleaner cleaner = new TimeToLiveLogCleaner();
    cleaner.setConf(conf);

    FileStatus mockStatus = new FileStatus(
        1024L, false, 1, 1024L, fileTime, new Path("hdfs://test/oldWALs/test-wal.12345"));

    // 3. Test code.
    boolean deletable = cleaner.isFileDeletable(mockStatus);

    // 4. Code after testing.
    assertFalse("File within TTL should not be deleted", deletable);
    EnvironmentEdgeManager.reset();
  }
}