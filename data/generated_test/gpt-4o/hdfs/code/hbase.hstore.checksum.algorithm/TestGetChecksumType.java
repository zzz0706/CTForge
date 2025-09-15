package org.apache.hadoop.hbase.regionserver;   

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({org.apache.hadoop.hbase.testclassification.SmallTests.class})
public class TestGetChecksumType {   

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestGetChecksumType.class);

    @Test
    public void test_getChecksumType_withValidConfiguration() {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values
        Configuration conf = new Configuration();

        // Prepare the test conditions: Set a checksum type in the configuration
        String checksumAlgorithm = ChecksumType.CRC32.name();
        conf.set(HConstants.CHECKSUM_TYPE_NAME, checksumAlgorithm);

        // 2. Test code: Use the `HStore.getChecksumType` method to get the checksum type from the configuration
        ChecksumType returnedChecksumType = HStore.getChecksumType(conf);

        // 3. Assertions: Verify that the returned checksum type matches the configured value
        assertEquals("The returned ChecksumType should match the configured value.",
                ChecksumType.nameToType(checksumAlgorithm), returnedChecksumType);

        // Code after testing: No cleanup required as no persistent change occurs
    }
}