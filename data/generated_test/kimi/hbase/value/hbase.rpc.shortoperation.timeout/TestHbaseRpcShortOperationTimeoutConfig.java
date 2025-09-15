package org.apache.hadoop.hbase.conf;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHbaseRpcShortOperationTimeoutConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHbaseRpcShortOperationTimeoutConfig.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUp() {
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testShortOperationTimeoutValid() {
    int timeout = conf.getInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
                              HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);

    // Must be a positive integer (>0)
    assertTrue("hbase.rpc.shortoperation.timeout must be > 0", timeout > 0);

    // Must be an integer (already ensured by getInt)
    assertTrue("hbase.rpc.shortoperation.timeout must be an integer",
               timeout == (int) timeout);
  }
}