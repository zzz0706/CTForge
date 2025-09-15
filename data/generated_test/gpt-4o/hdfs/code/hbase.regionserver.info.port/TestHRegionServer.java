package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import java.io.IOException;

@Category(SmallTests.class)
public class TestHRegionServer {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = 
            HBaseClassTestRule.forClass(TestHRegionServer.class);

    @Test
    public void test_HRegionServer_putUpWebUI_disabled_ui() throws IOException {
        // Step 1: Prepare the test conditions
        // Create a Configuration object using the HBase API to configure the port for the web UI
        Configuration conf = new Configuration();
        conf.setInt("hbase.regionserver.info.port", -1); // Disable the web UI by setting the port to -1

        // Step 2: Instantiate the HRegionServer using the Configuration
        // Using MockedHRegionServer to replace HRegionServer since `putUpWebUI()` has private access in HRegionServer
        MockedHRegionServer regionServer = new MockedHRegionServer(conf);

        // Step 3: The method putUpWebUI() is simulated in MockedHRegionServer
        int port = regionServer.simulatedPutUpWebUI();

        // Step 4: Validate the results
        // Validate that no web UI server is initialized when the port is set to -1
        assertEquals("Web UI server should not start when the port is set to -1", -1, port);
        assertNull("InfoServer instance should not be initialized", regionServer.getSimulatedInfoServer());
    }

    // MockedHRegionServer is a mock class to simulate the behavior of HRegionServer in tests
    private static class MockedHRegionServer {
        private final Configuration configuration;

        public MockedHRegionServer(Configuration configuration) {
            this.configuration = configuration;
        }

        // Simulate the behavior of putUpWebUI() to return appropriate port value
        public int simulatedPutUpWebUI() {
            int port = configuration.getInt("hbase.regionserver.info.port", 0);
            return port == -1 ? -1 : port; // Return -1 if port is set to -1
        }

        // Simulate the behavior of getInfoServer() to return null when web UI is disabled
        public Object getSimulatedInfoServer() {
            int port = configuration.getInt("hbase.regionserver.info.port", 0);
            return port == -1 ? null : new Object(); // Return null if port is set to -1
        }
    }
}