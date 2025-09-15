package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TimeToLiveLogCleanerTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TimeToLiveLogCleanerTest.class);

  @Test
  public void customTtlRespected() throws Exception {
    // 1. Instantiate Configuration and set custom TTL
    Configuration conf = new Configuration();
    long customTtl = 300000L;
    conf.setLong("hbase.master.logcleaner.ttl", customTtl);

    // 2. Instantiate TimeToLiveLogCleaner and inject configuration
    TimeToLiveLogCleaner cleaner = new TimeToLiveLogCleaner();
    cleaner.setConf(conf);

    // 3. Read back the TTL from configuration (dynamic expected value)
    long expectedTtl = conf.getLong("hbase.master.logcleaner.ttl", 600000L);

    // 4. Create a FileStatus whose modification time is 300001 ms in the past
    long now = System.currentTimeMillis();
    long fileModTime = now - (expectedTtl + 1);

    FileStatus mockFileStatus = mock(FileStatus.class);
    when(mockFileStatus.getModificationTime()).thenReturn(fileModTime);
    when(mockFileStatus.getPath()).thenReturn(
        new Path("hdfs://localhost:9000/hbase/oldWALs/test-wal"));

    // 5. Invoke isFileDeletable and assert the result
    boolean deletable = cleaner.isFileDeletable(mockFileStatus);
    assertTrue("File should be deletable when age exceeds custom TTL", deletable);
  }
}