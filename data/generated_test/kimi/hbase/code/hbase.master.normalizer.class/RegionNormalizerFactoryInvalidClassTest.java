package org.apache.hadoop.hbase.master.normalizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.fail;

@Category({MasterTests.class, SmallTests.class})
public class RegionNormalizerFactoryInvalidClassTest {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(RegionNormalizerFactoryInvalidClassTest.class);

    @Test
    public void testInvalidClassThrowsRuntimeException() {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        // 2. Prepare the test conditions.
        conf.set(HConstants.HBASE_MASTER_NORMALIZER_CLASS, "com.example.NonExistentNormalizer");
        // 3. Test code.
        try {
            RegionNormalizerFactory.getRegionNormalizer(conf);
            fail("Expected RuntimeException");
        } catch (RuntimeException expected) {
            // Expected
        }
        // 4. Code after testing.
    }
}