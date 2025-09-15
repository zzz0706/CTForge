package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestBucketCacheBucketSizes {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestBucketCacheBucketSizes.class);

    @Test
    public void testBucketCacheBucketSizesConfiguration() {
        Configuration config = HBaseConfiguration.create();
        String[] bucketSizesStr = config.getStrings("hbase.bucketcache.bucket.sizes", new String[0]);
        if (bucketSizesStr.length == 0) {
            return;
        }
        for (String bucketSizeStr : bucketSizesStr) {
            int bucketSize = Integer.parseInt(bucketSizeStr.trim());
            assertTrue("Bucket size must be a multiple of 256: " + bucketSize,
                       bucketSize % 256 == 0);
        }
    }
}
