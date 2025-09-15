package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category(SmallTests.class)
public class TestCompactionConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionConfiguration.class);

  @Test
  public void testMinCompactSizeDefaultFromMemStoreFlushSize() {
    // 1. Configuration as input
    Configuration conf = new Configuration(); // do NOT set hbase.hstore.compaction.min.size

    // 2. Dynamic expected value calculation
    long expectedMemStoreFlushSize = 67108864L; // 64 MB in bytes

    // 3. Mock/Stub external dependencies
    StoreConfigInformation storeConfigInfo = mock(StoreConfigInformation.class);
    when(storeConfigInfo.getMemStoreFlushSize()).thenReturn(expectedMemStoreFlushSize);

    // 4. Invoke the method under test
    CompactionConfiguration compactionConf = new CompactionConfiguration(conf, storeConfigInfo);
    long actualMinCompactSize = compactionConf.getMinCompactSize();

    // 5. Assertions and verification
    assertEquals(expectedMemStoreFlushSize, actualMinCompactSize);
  }
}