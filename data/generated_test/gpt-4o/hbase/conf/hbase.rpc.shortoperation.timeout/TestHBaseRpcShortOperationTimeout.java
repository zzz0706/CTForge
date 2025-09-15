package org.apache.hadoop.hbase.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestHBaseRpcShortOperationTimeout {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestHBaseRpcShortOperationTimeout.class);

    @Test
    public void testShortOperationTimeoutConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        int timeout = conf.getInt("hbase.rpc.shortoperation.timeout", 10000);
        assertTrue(timeout > 0);
        assertTrue(timeout <= 60000);
    }
}
