package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

//HBASE-13320
@Category(SmallTests.class)
public class TestBucketCacheSizeConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestBucketCacheSizeConfig.class);

    /**
     * Test to check the format and validity of hbase.bucketcache.size configuration value.
     */
    @Test
    public void testBucketCacheSizeConfigFormat() {
        Configuration conf = new Configuration();
        String sizeStr = conf.get("hbase.bucketcache.size", "").trim();

        // Allow unset (empty), which means use system default, so considered valid
        if (sizeStr.isEmpty()) return;

        boolean valid = false;

        try {
            // Try parsing as float (e.g., 0.5)
            float floatVal = Float.parseFloat(sizeStr);
            if (floatVal > 0.0f && floatVal < 1.0f) {
                valid = true;
            }
        } catch (NumberFormatException ignored) {
            // Not a float, try integer (MB)
            try {
                int intVal = Integer.parseInt(sizeStr);
                if (intVal > 0) {
                    valid = true;
                }
            } catch (NumberFormatException ignored2) {
                // Not an integer either
            }
        }

        assertTrue(
            "hbase.bucketcache.size must be a positive float less than 1.0 (ratio), or a positive integer (MBs). " +
            "Found: " + sizeStr,
            valid
        );
    }
}
