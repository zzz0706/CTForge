package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@Category({MasterTests.class, SmallTests.class})
public class TestHBaseMasterPortConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestHBaseMasterPortConfig.class);

    @Test
    public void testMasterPortWithinValidRange() {
        Configuration conf = HBaseConfiguration.create();
        int port = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);
        assertTrue(port >= 0 && port <= 65535);
    }

}
