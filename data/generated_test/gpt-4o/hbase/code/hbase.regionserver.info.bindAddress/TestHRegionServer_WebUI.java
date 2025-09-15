package org.apache.hadoop.hbase.regionserver;   

import org.apache.hadoop.hbase.HBaseClassTestRule;      
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.Assert;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionServer_WebUI {   
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestHRegionServer_WebUI.class);

    @Test
    public void testHRegionServer_WebUI_StartWithCustomBindAddress() throws Exception {
        // Test code
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        HBaseTestingUtility testingUtility = new HBaseTestingUtility();

        try {
            // 2. Prepare the test conditions: Start the mini cluster
            Configuration conf = testingUtility.getConfiguration();
            conf.setInt(HConstants.REGIONSERVER_INFO_PORT, 0); // Let the server use an ephemeral port
            conf.set("hbase.regionserver.info.bindAddress", "127.0.0.1");
            testingUtility.startMiniCluster(1); // Start a cluster with 1 RegionServer
            
            // 3. Test code: Verify that the RegionServer has correctly started with the custom bind address
            JVMClusterUtil.RegionServerThread rsThread = testingUtility.getMiniHBaseCluster().getRegionServerThreads().get(0);
            HRegionServer rs = (HRegionServer) rsThread.getRegionServer();
            ServerName serverName = rs.getServerName();
            
            // Ensure the RegionServer is running
            Assert.assertTrue("RegionServer should be running.", rs.isOnline());

            // Check that the web UI is accessible at the expected port
            int webUIPort = rs.getConfiguration().getInt(HConstants.REGIONSERVER_INFO_PORT, -1);
            Assert.assertTrue("The web UI should start on a valid port.", webUIPort > 0);

            System.out.println("RegionServer Web UI started on port: " + webUIPort);

        } finally {
            // 4. Code after testing: Cleanup the testing environment
            testingUtility.shutdownMiniCluster();
        }
    }
}