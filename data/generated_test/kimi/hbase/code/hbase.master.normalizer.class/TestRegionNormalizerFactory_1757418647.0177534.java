package org.apache.hadoop.hbase.master.normalizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category({MasterTests.class, SmallTests.class})
public class TestRegionNormalizerFactory {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestRegionNormalizerFactory.class);

  @Test
  public void testDefaultNormalizerIsInstantiatedWhenPropertyAbsent() {
    // 1. Create a fresh Configuration without touching the property
    Configuration conf = new Configuration();

    // 2. Invoke the factory method
    RegionNormalizer normalizer = RegionNormalizerFactory.getRegionNormalizer(conf);

    // 3. Assert the actual instance type matches the expected default
    assertNotNull(normalizer);
    assertEquals(SimpleRegionNormalizer.class, normalizer.getClass());
  }

  @Test
  public void testCustomNormalizerIsInstantiatedWhenPropertyPresent() {
    // 1. Create a Configuration and set a custom normalizer class
    Configuration conf = new Configuration();
    conf.setClass(HConstants.HBASE_MASTER_NORMALIZER_CLASS, DummyRegionNormalizer.class, RegionNormalizer.class);

    // 2. Invoke the factory method
    RegionNormalizer normalizer = RegionNormalizerFactory.getRegionNormalizer(conf);

    // 3. Assert the actual instance type matches the configured class
    assertNotNull(normalizer);
    assertEquals(DummyRegionNormalizer.class, normalizer.getClass());
  }

  public static class DummyRegionNormalizer implements RegionNormalizer {
    @Override
    public void setMasterServices(HMaster masterServices) {}
    @Override
    public void setMasterRpcServices(org.apache.hadoop.hbase.master.MasterRpcServices masterRpcServices) {}
    @Override
    public void start() {}
    @Override
    public void stop() {}
    @Override
    public boolean isRunning() { return false; }
    @Override
    public void planNormalization(org.apache.hadoop.hbase.master.MasterServices masterServices,
                                  org.apache.hadoop.hbase.TableName tableName) {}
  }
}