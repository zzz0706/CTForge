package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category({MasterTests.class, SmallTests.class})
public class TestTimeToLiveLogCleanerDefaultTtl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTimeToLiveLogCleanerDefaultTtl.class);

  @Test
  public void defaultTtlUsedWhenNoOverride() {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    long expectedTtl = conf.getLong(TimeToLiveLogCleaner.TTL_CONF_KEY, 600000L);

    // 2. Prepare the test conditions.
    TimeToLiveLogCleaner cleaner = new TimeToLiveLogCleaner();
    cleaner.setConf(conf);

    final long now = System.currentTimeMillis();
    final long fileTime = now - expectedTtl - 1; // exactly DEFAULT_TTL + 1 ms in the past

    FileStatus mockFileStatus = mock(FileStatus.class);
    when(mockFileStatus.getPath()).thenReturn(new Path("hdfs://test/oldWALs/test-wal"));
    when(mockFileStatus.getModificationTime()).thenReturn(fileTime);

    EnvironmentEdge edge = new EnvironmentEdge() {
      @Override
      public long currentTime() {
        return now;
      }
    };
    EnvironmentEdgeManager.injectEdge(edge);

    try {
      // 3. Test code.
      boolean deletable = cleaner.isFileDeletable(mockFileStatus);

      // 4. Code after testing.
      assertTrue("File should be deletable when age exceeds default TTL", deletable);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }
}