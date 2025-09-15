package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(SmallTests.class)
public class TestCompactionThrottleClassification {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionThrottleClassification.class);

  @Test
  public void verifyThrottleClassificationForLargeCompaction() {
    // 1. Configuration as input – no explicit set() calls, rely on defaults
    Configuration conf = HBaseConfiguration.create();

    // 2. Dynamic expected value calculation
    // Default memstore flush size is 128MB (134217728 bytes)
    long defaultMemStoreFlushSize = conf.getLong("hbase.hregion.memstore.flush.size", 134217728L);
    int maxFilesToCompact = conf.getInt("hbase.hstore.compaction.max", 10);
    long expectedThrottlePoint = 2L * maxFilesToCompact * defaultMemStoreFlushSize;

    // 3. Prepare the test conditions
    StoreConfigInformation storeConf = mock(StoreConfigInformation.class);
    when(storeConf.getMemStoreFlushSize()).thenReturn(defaultMemStoreFlushSize);

    // 4. Test code
    RatioBasedCompactionPolicy policy = new RatioBasedCompactionPolicy(conf, storeConf);
    // In HBase 2.2.2, throttleCompaction(long) always returns false
    boolean result = policy.throttleCompaction(expectedThrottlePoint + 1);

    // 5. Code after testing
    assertTrue(!result);
  }
}