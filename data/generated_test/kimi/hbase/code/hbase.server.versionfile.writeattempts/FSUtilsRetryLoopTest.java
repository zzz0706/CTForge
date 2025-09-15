package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@Category({MiscTests.class, SmallTests.class})
public class FSUtilsRetryLoopTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(FSUtilsRetryLoopTest.class);

  @ClassRule
  public static final TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void testRetryLoopExecutesExactlyConfiguredTimes() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    int expectedRetries = conf.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
                                      HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);

    // 2. Prepare the test conditions.
    FileSystem fs = mock(FileSystem.class);
    Path rootPath = new Path(testFolder.getRoot().getAbsolutePath(), "hbase");

    // Stub FileSystem.create to throw IOException on every attempt
    when(fs.create(any(Path.class))).thenThrow(new IOException("Mocked IO failure"));

    // 3. Test code.
    try {
      FSUtils.setVersion(fs, rootPath, 10, expectedRetries);
    } catch (IOException ignored) {
      // Expected to throw after retries exhausted
    }

    // 4. Code after testing.
    // verify the number of times fs.create was called
    verify(fs, times(expectedRetries + 1)).create(any(Path.class));
  }
}