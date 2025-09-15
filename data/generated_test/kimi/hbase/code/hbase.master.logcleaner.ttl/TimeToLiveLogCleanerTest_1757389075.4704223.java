package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertFalse;
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
  public void negativeLifeSkipsDeletion() {
    // 1. Obtain configuration value via hbase 2.2.2 API
    Configuration conf = new Configuration();
    long ttl = conf.getLong(TimeToLiveLogCleaner.TTL_CONF_KEY, 600000L);

    // 2. Prepare test conditions
    TimeToLiveLogCleaner cleaner = new TimeToLiveLogCleaner();
    cleaner.setConf(conf);

    FileStatus mockFileStatus = mock(FileStatus.class);
    Path mockPath = mock(Path.class);
    when(mockPath.getName()).thenReturn("logfile.1234567890");
    when(mockFileStatus.getPath()).thenReturn(mockPath);
    long futureTime = System.currentTimeMillis() + 1;
    when(mockFileStatus.getModificationTime()).thenReturn(futureTime);

    // 3. Test code
    boolean deletable = cleaner.isFileDeletable(mockFileStatus);

    // 4. Post-test verification
    assertFalse("File with future modification time should not be deleted", deletable);
  }

  @Test
  public void expiredFileIsDeletable() {
    // 1. Obtain configuration value via hbase 2.2.2 API
    Configuration conf = new Configuration();
    conf.setLong(TimeToLiveLogCleaner.TTL_CONF_KEY, 1L); // 1 ms TTL

    // 2. Prepare test conditions
    TimeToLiveLogCleaner cleaner = new TimeToLiveLogCleaner();
    cleaner.setConf(conf);

    FileStatus mockFileStatus = mock(FileStatus.class);
    Path mockPath = mock(Path.class);
    when(mockPath.getName()).thenReturn("logfile.1234567890");
    when(mockFileStatus.getPath()).thenReturn(mockPath);
    long pastTime = System.currentTimeMillis() - 1000; // 1 second ago
    when(mockFileStatus.getModificationTime()).thenReturn(pastTime);

    // 3. Test code
    boolean deletable = cleaner.isFileDeletable(mockFileStatus);

    // 4. Post-test verification
    assertTrue("Expired file should be deletable", deletable);
  }

  @Test
  public void invalidWALFilenameIsDeletable() {
    // 1. Obtain configuration value via hbase 2.2.2 API
    Configuration conf = new Configuration();

    // 2. Prepare test conditions
    TimeToLiveLogCleaner cleaner = new TimeToLiveLogCleaner();
    cleaner.setConf(conf);

    FileStatus mockFileStatus = mock(FileStatus.class);
    Path mockPath = mock(Path.class);
    when(mockPath.getName()).thenReturn("invalid_filename");
    when(mockFileStatus.getPath()).thenReturn(mockPath);

    // 3. Test code
    boolean deletable = cleaner.isFileDeletable(mockFileStatus);

    // 4. Post-test verification
    assertTrue("Invalid WAL filename should be deletable", deletable);
  }
}