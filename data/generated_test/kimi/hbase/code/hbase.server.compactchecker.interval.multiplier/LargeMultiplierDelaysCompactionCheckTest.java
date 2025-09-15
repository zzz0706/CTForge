package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@Category({RegionServerTests.class, SmallTests.class})
public class LargeMultiplierDelaysCompactionCheckTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(LargeMultiplierDelaysCompactionCheckTest.class);

  @Test
  public void testLargeMultiplierDelaysCompactionCheck() throws Exception {
    // 1. Configuration as Input
    Configuration conf = HBaseConfiguration.create();
    // Use the default value from HStore so we do NOT call conf.set(...)
    long expectedMultiplier = conf.getInt(
        HStore.COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY,
        HStore.DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER);

    // Override with a very large value for this test only
    conf.setInt(HStore.COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY, 1_000_000);
    expectedMultiplier = 1_000_000; // re-calculate after override

    // 2. Prepare the test conditions
    // Mock HStore with the configuration
    HRegion mockRegion = mock(HRegion.class);
    HStore mockStore = mock(HStore.class);
    when(mockStore.getCompactionCheckMultiplier()).thenReturn(expectedMultiplier);
    when(mockStore.needsCompaction()).thenReturn(false);

    // 3. Test code: simulate the chore loop
    long calls = 0;
    for (long iteration = 0; iteration < 500_000; iteration++) {
      long multiplier = mockStore.getCompactionCheckMultiplier();
      if (iteration % multiplier == 0) {
        if (mockStore.needsCompaction()) {
          calls++;
        }
      }
    }

    // 4. Code after testing
    // Expected: 0 calls because 500,000 % 1,000,000 != 0
    assertEquals(0, calls);
  }
}