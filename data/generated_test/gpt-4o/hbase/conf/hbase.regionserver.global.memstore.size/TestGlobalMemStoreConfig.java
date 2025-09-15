package org.apache.hadoop.hbase.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for validating HBase configuration constraints related to
 * hbase.regionserver.global.memstore size and limits.
 */
@Category(SmallTests.class) // Use category annotation for test classification
public class TestGlobalMemStoreConfig {

  // Class rule for test environment setup
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestGlobalMemStoreConfig.class);

  @Test
  public void testGlobalMemStoreSize() {
    // 1. Prepare the test conditions.
    Configuration conf = new Configuration();

    // 2. Use the HBase 2.2.2 API correctly to obtain the configuration value.
    float globalMemstoreLimit = MemorySizeUtil.getGlobalMemStoreHeapPercent(conf, true);

    // 3. Verify that the global memstore limit falls within the specified valid range (0.0, 0.8].
    assertTrue("Expected global memstore limit to be within (0.0, 0.8]", globalMemstoreLimit > 0.0f && globalMemstoreLimit <= 0.8f);
  }


}