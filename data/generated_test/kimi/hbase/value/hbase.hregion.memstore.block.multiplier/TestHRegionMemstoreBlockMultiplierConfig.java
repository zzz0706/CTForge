package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionMemstoreBlockMultiplierConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHRegionMemstoreBlockMultiplierConfig.class);

  @Test
  public void testHRegionMemstoreBlockMultiplierConfigValidation() {
    // 1. Obtain the configuration without hard-coding any value
    Configuration conf = new Configuration();
    conf.addResource("hbase-site.xml");

    // 2. Retrieve the value of hbase.hregion.memstore.block.multiplier
    long multiplier = conf.getLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER,
        HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER);

    // 3. Validate the value
    // According to usage in HRegion#setHTableSpecificConf, the multiplier is multiplied
    // with memstoreFlushSize to derive blockingMemStoreSize.
    // Therefore it must be a positive integer; zero or negative would make blockingMemStoreSize
    // <= 0, which would immediately block every update or cause unexpected behavior.
    if (multiplier <= 0) {
      fail("Invalid value for " + HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER +
           ": " + multiplier + ". Value must be a positive integer.");
    }
  }
}