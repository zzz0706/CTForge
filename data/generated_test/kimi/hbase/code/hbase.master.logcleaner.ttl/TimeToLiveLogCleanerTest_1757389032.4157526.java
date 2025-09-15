package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({MasterTests.class, SmallTests.class})
public class TimeToLiveLogCleanerTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TimeToLiveLogCleanerTest.class);

  @Test
  public void negativeLifeSkipsDeletion() {
    // 1. Instantiate Configuration and read default TTL
    Configuration conf = new Configuration();
    long expectedTtl = conf.getLong(TimeToLiveLogCleaner.TTL_CONF_KEY, 600000L);

    // 2. Instantiate TimeToLiveLogCleaner and feed the configuration
    TimeToLiveLogCleaner cleaner = new TimeToLiveLogCleaner();
    cleaner.setConf(conf);

    // 3. Create a FileStatus whose modification time is 1 ms in the future
    FileStatus mockFileStatus = mock(FileStatus.class);
    Path mockPath = mock(Path.class);
    when(mockPath.getName()).thenReturn("logfile.1234567890");
    when(mockFileStatus.getPath()).thenReturn(mockPath);
    // Current time + 1 ms
    long futureTime = System.currentTimeMillis() + 1;
    when(mockFileStatus.getModificationTime()).thenReturn(futureTime);

    // 4. Call isFileDeletable and assert
    boolean deletable = cleaner.isFileDeletable(mockFileStatus);
    assertFalse("File with future modification time should not be deleted", deletable);
  }
}