package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category({MasterTests.class, SmallTests.class})
public class TimeToLiveLogCleanerTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TimeToLiveLogCleanerTest.class);

  @Test
  public void invalidWALNameAlwaysDeletable() throws Exception {
    // 1. Configuration as input – use defaults
    Configuration conf = new Configuration();

    // 2. Instantiate the cleaner and feed the configuration
    TimeToLiveLogCleaner cleaner = new TimeToLiveLogCleaner();
    cleaner.setConf(conf);

    // 3. Create a FileStatus for a file with an invalid WAL name
    FileStatus mockFileStatus = mock(FileStatus.class);
    when(mockFileStatus.getPath()).thenReturn(new Path("hdfs://hbase/oldWALs/invalid.log"));
    when(mockFileStatus.getModificationTime()).thenReturn(System.currentTimeMillis() - 1L);

    // 4. Invoke the method under test
    boolean deletable = cleaner.isFileDeletable(mockFileStatus);

    // 5. Assert the expected behavior
    assertTrue("Files with invalid WAL names should always be considered deletable", deletable);
  }
}