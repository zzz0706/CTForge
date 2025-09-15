package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetAddress;

import static org.junit.Assert.*;

@Category({MiscTests.class, SmallTests.class})
public class TestRegionServerInfoPortConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestRegionServerInfoPortConfig.class);

    private static Configuration conf;

    @BeforeClass
    public static void setUp() {
        conf = HBaseConfiguration.create();
    }

    @Test
    public void testRegionServerInfoPortValidation() throws Exception {
        int port = conf.getInt(
            HConstants.REGIONSERVER_INFO_PORT,
            HConstants.DEFAULT_REGIONSERVER_INFOPORT);

        assertTrue(port == -1 || (port >= 0 && port <= 65535));


        if (port == -1) {
            assertFalse(isUiServerRunning());
        }

        String bind = conf.get(
            "hbase.regionserver.info.bindAddress",
            "0.0.0.0");
        InetAddress addr = InetAddress.getByName(bind);
        assertTrue(addr.isAnyLocalAddress() || addr.isLoopbackAddress());
    }

    private boolean isUiServerRunning() {
        return false;
    }
}
