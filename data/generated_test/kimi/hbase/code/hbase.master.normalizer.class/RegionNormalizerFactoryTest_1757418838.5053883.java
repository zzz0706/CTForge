package org.apache.hadoop.hbase.master.normalizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

@Category({MasterTests.class, SmallTests.class})
public class RegionNormalizerFactoryTest {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(RegionNormalizerFactoryTest.class);

    public static class MyTestNormalizer implements RegionNormalizer {
        @Override
        public void setMasterServices(MasterServices masterServices) {}
        @Override
        public void setMasterRpcServices(MasterRpcServices masterRpcServices) {}
    }

    @Test
    public void testCustomNormalizerIsInstantiatedWhenPropertySet() {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Dynamic Expected Value Calculation
        Class<? extends RegionNormalizer> expectedClass = MyTestNormalizer.class;
        conf.setClass(HConstants.HBASE_MASTER_NORMALIZER_CLASS, expectedClass, RegionNormalizer.class);

        // 3. Mock/Stub External Dependencies – none needed for this test

        // 4. Invoke the Method Under Test
        RegionNormalizer normalizer = RegionNormalizerFactory.getRegionNormalizer(conf);

        // 5. Assertions and Verification
        assertNotNull(normalizer);
        assertEquals(expectedClass, normalizer.getClass());
    }
}