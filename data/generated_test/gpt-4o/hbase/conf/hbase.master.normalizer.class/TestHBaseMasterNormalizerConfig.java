package org.apache.hadoop.hbase.master.normalizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Unit test for validating the configuration value of
 * 'hbase.master.normalizer.class' in HBase 2.2.2.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestHBaseMasterNormalizerConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestHBaseMasterNormalizerConfig.class);

    @Test
    public void testNormalizerClassConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        String key = HConstants.HBASE_MASTER_NORMALIZER_CLASS;
        String defaultCls = SimpleRegionNormalizer.class.getName();
        String clsName = conf.get(key, defaultCls);

        try {
            Class<?> clazz = conf.getClassByName(clsName);
            assertTrue("Class must implement RegionNormalizer",
                RegionNormalizer.class.isAssignableFrom(clazz));
            RegionNormalizer normalizer = (RegionNormalizer)
                ReflectionUtils.newInstance(clazz, conf);
            assertNotNull(normalizer);
        } catch (ClassNotFoundException e) {
            fail("Normalizer class not found: " + clsName);
        } catch (ClassCastException e) {
            fail("Invalid implementation for RegionNormalizer: " + clsName);
        }
    }
}
