package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category(SmallTests.class)
public class HStoreChecksumAlgorithmTest {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(HStoreChecksumAlgorithmTest.class);

    @Test(expected = RuntimeException.class)
    public void testInvalidChecksumTypeThrowsRuntimeException() {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        conf.set(HConstants.CHECKSUM_TYPE_NAME, "MD5");

        // 3. Test code.
        HStore.getChecksumType(conf);

        // 4. Code after testing.
        // (Exception is expected, so no additional cleanup needed.)
    }
}