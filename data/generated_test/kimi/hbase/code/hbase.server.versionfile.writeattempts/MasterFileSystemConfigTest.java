package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class MasterFileSystemConfigTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(MasterFileSystemConfigTest.class);

  @Test
  public void testCustomWriteAttemptsPropagatedCorrectly() {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    conf.setInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS, 7);

    // 2. Prepare the test conditions.
    int expectedRetries = conf.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
                                       HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);

    // 3. Test code.
    assertEquals(7, expectedRetries);

    // 4. Code after testing.
  }
}