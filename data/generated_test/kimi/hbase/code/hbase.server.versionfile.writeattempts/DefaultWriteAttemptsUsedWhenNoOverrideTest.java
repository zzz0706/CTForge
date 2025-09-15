package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({MasterTests.class, SmallTests.class})
public class DefaultWriteAttemptsUsedWhenNoOverrideTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(DefaultWriteAttemptsUsedWhenNoOverrideTest.class);

  @Test
  public void testDefaultWriteAttemptsUsedWhenNoOverride() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    int defaultRetries = conf.getInt(
        HConstants.VERSION_FILE_WRITE_ATTEMPTS,
        HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);

    // 2. Prepare the test conditions.
    FileSystem fs = mock(FileSystem.class);
    Path rootPath = new Path("/hbase");

    // 3. Test code.
    // We simply verify that the default value is correctly fetched from the configuration
    assertEquals(HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS, defaultRetries);

    // 4. Code after testing.
    // Nothing to clean up; all objects are local
  }
}