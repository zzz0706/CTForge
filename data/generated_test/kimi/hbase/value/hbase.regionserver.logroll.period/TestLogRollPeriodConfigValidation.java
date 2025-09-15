package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category({RegionServerTests.class, SmallTests.class})
public class TestLogRollPeriodConfigValidation {

    private static Configuration conf;

    @ClassRule
    public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
            org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestLogRollPeriodConfigValidation.class);

    @BeforeClass
    public static void setUpBeforeClass() {
        // 1. Use the HBase 2.2.2 API to obtain configuration values.
        conf = HBaseConfiguration.create();
    }

    @Test
    public void testLogRollPeriodValid() {
        // 2. Prepare the test conditions.
        long rollPeriod = conf.getLong("hbase.regionserver.logroll.period", 3600000L);

        // 3. Test code.
        // Constraint: must be a positive long (> 0)
        assertTrue("hbase.regionserver.logroll.period must be a positive long value",
                rollPeriod > 0);

        // 4. Code after testing.
        // (Nothing required for this test)
    }
}