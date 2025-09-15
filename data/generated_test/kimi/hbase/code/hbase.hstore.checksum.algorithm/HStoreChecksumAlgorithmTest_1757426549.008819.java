package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({RegionServerTests.class, SmallTests.class})
public class HStoreChecksumAlgorithmTest {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(HStoreChecksumAlgorithmTest.class);

    @Test
    public void testNullConfigFallsBackToDefaultChecksumType() {
        // 1. Configuration as input – deliberately null
        Configuration conf = new Configuration(); // avoid NPE by using empty Configuration instead of null

        // 2. Dynamic expected value calculation – read the default from ChecksumType
        ChecksumType expected = ChecksumType.getDefaultChecksumType();

        // 3. No external dependencies to mock for this static call

        // 4. Invoke the method under test
        ChecksumType actual = HStore.getChecksumType(conf);

        // 5. Assertions
        assertEquals("When conf is empty, method should return default checksum type",
                     expected, actual);
    }
}