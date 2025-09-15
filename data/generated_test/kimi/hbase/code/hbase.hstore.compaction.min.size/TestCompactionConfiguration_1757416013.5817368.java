package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class TestCompactionConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionConfiguration.class);

  @Test
  public void testMinCompactSizeOverridesMemStoreFlushSize() {
    // 1. Instantiate Configuration
    Configuration conf = HBaseConfiguration.create();
    // 2. Explicitly set the property to a non-default value
    conf.setLong("hbase.hstore.compaction.min.size", 33554432L);
    // 3. Mock StoreConfigInfo to return any value (will be ignored)
    StoreConfigInformation mockStoreConfigInfo = Mockito.mock(StoreConfigInformation.class);
    Mockito.when(mockStoreConfigInfo.getMemStoreFlushSize()).thenReturn(134217728L);

    // 4. Create CompactionConfiguration
    CompactionConfiguration compactionConfig =
        new CompactionConfiguration(conf, mockStoreConfigInfo);

    // 5. Capture the returned value
    long actualMinCompactSize = compactionConfig.getMinCompactSize();

    // 6. Assert the expected value
    assertEquals(33554432L, actualMinCompactSize);
  }
}