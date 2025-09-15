package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;

import static org.junit.Assert.assertEquals;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHStore {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestHStore.class);

    @Test
    public void test_getChecksumType_withDefaultConfiguration() {
        // 1. Prepare the test conditions.
        Configuration configuration = new Configuration(); // Creating a Configuration object without setting the `hbase.hstore.checksum.algorithm`.

        // 2. Test code: Invoke the `HStore.getChecksumType` method and capture the result.
        ChecksumType actualChecksumType = HStore.getChecksumType(configuration);

        // 3. Verify the expected result.
        ChecksumType expectedChecksumType = ChecksumType.getDefaultChecksumType(); // The default value provided by the ChecksumType utility.
        assertEquals("The returned ChecksumType should match the default ChecksumType.", expectedChecksumType, actualChecksumType);
    }
}