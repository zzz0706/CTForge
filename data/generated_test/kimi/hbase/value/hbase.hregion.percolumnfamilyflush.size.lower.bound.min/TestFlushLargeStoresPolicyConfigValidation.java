package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category(SmallTests.class)
public class TestFlushLargeStoresPolicyConfigValidation {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestFlushLargeStoresPolicyConfigValidation.class);

    private static Configuration conf;

    @BeforeClass
    public static void setUp() {
        conf = HBaseConfiguration.create();
    }

    @Test
    public void testLowerBoundMinConfigIsPositive() {
        long lowerBoundMin = conf.getLong(
                "hbase.hregion.percolumnfamilyflush.size.lower.bound.min",
                16L * 1024 * 1024); // 16MB default
        assertTrue("hbase.hregion.percolumnfamilyflush.size.lower.bound.min must be > 0",
                lowerBoundMin > 0);
    }

    @Test
    public void testLowerBoundMinConfigIsParsableLong() {
        String valueStr = conf.get(
                "hbase.hregion.percolumnfamilyflush.size.lower.bound.min",
                "16777216");
        try {
            Long.parseLong(valueStr);
        } catch (NumberFormatException e) {
            fail("hbase.hregion.percolumnfamilyflush.size.lower.bound.min must be a valid long integer");
        }
    }
}