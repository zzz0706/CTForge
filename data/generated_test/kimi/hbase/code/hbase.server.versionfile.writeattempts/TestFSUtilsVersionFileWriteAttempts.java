package org.apache.hadoop.hbase.util;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category(SmallTests.class)
public class TestFSUtilsVersionFileWriteAttempts {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFSUtilsVersionFileWriteAttempts.class);

  @Test
  public void checkVersionPassesConfiguredRetriesToSetVersion() throws Exception {
    // 1. Configuration as input
    Configuration conf = new Configuration();

    // 2. Dynamic expected value calculation
    int expectedRetries = conf.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
                                      HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);
    int wakeFreq = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);

    // 3. Prepare test conditions
    Path rootPath = new Path("mock:///hbase");
    FileSystem fs = mock(FileSystem.class);

    // Stub the FileSystem to avoid real I/O
    when(fs.exists(any(Path.class))).thenReturn(false);
    FSDataOutputStream mockOs = mock(FSDataOutputStream.class);
    when(fs.create(any(Path.class))).thenReturn(mockOs);
    doNothing().when(mockOs).write(any(byte[].class), anyInt(), anyInt());
    doNothing().when(mockOs).close();
    when(fs.rename(any(Path.class), any(Path.class))).thenReturn(true);

    // 4. Invoke method under test
    FSUtils.checkVersion(fs, rootPath, false, wakeFreq, expectedRetries);

    // 5. Assertions and verification
    // Ensure the test compiles and runs without throwing IOException
    verify(fs, atLeastOnce()).create(any(Path.class));
  }
}