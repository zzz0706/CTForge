package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;

@Category(SmallTests.class)
public class TestRWQueueRpcExecutorConfigValidation {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestRWQueueRpcExecutorConfigValidation.class);

  /**
   * Validate that hbase.ipc.server.callqueue.read.ratio is a float in [0.0, 1.0].
   */
  @Test
  public void testReadRatioValidation() {
    Configuration conf = HBaseConfiguration.create();
    // Do NOT set any value in code; rely on loaded hbase-site.xml / hbase-default.xml
    float ratio = conf.getFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0f);

    if (Float.isNaN(ratio)) {
      fail("hbase.ipc.server.callqueue.read.ratio is NaN");
    }
    if (ratio < 0f || ratio > 1f) {
      fail("hbase.ipc.server.callqueue.read.ratio must be between 0.0 and 1.0, found: " + ratio);
    }
    assertTrue("hbase.ipc.server.callqueue.read.ratio is valid", true);
  }
}