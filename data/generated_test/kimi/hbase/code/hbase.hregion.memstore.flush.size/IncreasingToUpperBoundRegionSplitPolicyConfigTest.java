package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class IncreasingToUpperBoundRegionSplitPolicyConfigTest {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(IncreasingToUpperBoundRegionSplitPolicyConfigTest.class);

  @Test
  public void testSplitPolicyInitialSizeUsesConfiguredFlushSize() throws Exception {
    // 1. Instantiate Configuration
    Configuration conf = new Configuration();

    // 2. Dynamic expected value calculation
    long flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
                                  TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE);
    long expectedInitialSize = 2 * flushSize;

    // 3. Mock external dependencies
    HRegion mockRegion = mock(HRegion.class);
    TableDescriptor mockDescriptor = null; // force fallback to conf
    when(mockRegion.getTableDescriptor()).thenReturn(mockDescriptor);

    // 4. Invoke method under test
    IncreasingToUpperBoundRegionSplitPolicy policy =
        new IncreasingToUpperBoundRegionSplitPolicy();
    policy.setConf(conf);
    policy.configureForRegion(mockRegion);

    // 5. Assertions
    assertEquals("initialSize should be 2 * flushSize from configuration",
                 expectedInitialSize, policy.initialSize);
  }
}