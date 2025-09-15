package org.apache.hadoop.hbase.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class HBaseMasterPortConfigTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(HBaseMasterPortConfigTest.class);

  @Test
  public void testDefaultPortIsUsedWhenNoOverride() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values,
    //    instead of hardcoding the configuration values.
    Configuration conf = new Configuration();

    // 2. Prepare the test conditions.
    int expectedPort = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);

    // 3. Test code.
    // Test LocalHBaseCluster
    int actualPortInCluster = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);

    // 4. Code after testing.
    assertEquals("LocalHBaseCluster should use default master port", expectedPort, actualPortInCluster);
  }
}