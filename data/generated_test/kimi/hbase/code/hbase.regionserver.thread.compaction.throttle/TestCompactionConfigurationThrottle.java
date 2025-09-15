package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class TestCompactionConfigurationThrottle {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionConfigurationThrottle.class);

  @Test
  public void verifyDefaultThrottlePointComputation() {
    // 1. Create a fresh Configuration without explicit overrides
    Configuration conf = new Configuration();

    // 2. Mock StoreConfigInformation to return 128 MB for memstore flush size
    StoreConfigInformation mockStoreInfo = Mockito.mock(StoreConfigInformation.class);
    Mockito.when(mockStoreInfo.getMemStoreFlushSize()).thenReturn(134217728L);

    // 3. Instantiate CompactionConfiguration using the mocked dependencies
    CompactionConfiguration cc = new CompactionConfiguration(conf, mockStoreInfo);

    // 4. Obtain the actual throttle point
    long actual = cc.getThrottlePoint();

    // 5. Compute expected value: 2 * maxFilesToCompact * memStoreFlushSize
    int maxFilesToCompact = conf.getInt("hbase.hstore.compaction.max", 10);
    long memStoreFlushSize = mockStoreInfo.getMemStoreFlushSize();
    long expected = 2L * maxFilesToCompact * memStoreFlushSize;

    // 6. Assert the computed throttle point
    assertEquals(expected, actual);
  }
}