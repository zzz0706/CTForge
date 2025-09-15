package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hdfs.DFSUtilClient;

import org.junit.Test;
import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.Map;

public class TestDFSUtilClient {

    @Test
    // test code
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getHaNnWebHdfsAddresses_webhdfs_scheme() {
        // Step 1: Prepare the configuration object for the test.
        Configuration conf = new Configuration();

        // Set the configuration for enabling HA and specify the nameservice and NameNodes.
        conf.set("dfs.nameservices", "nameservice1");
        conf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
        conf.set("dfs.namenode.http-address.nameservice1.nn1", "localhost:50070");
        conf.set("dfs.namenode.http-address.nameservice1.nn2", "localhost:50071");

        // Step 2: Use the HDFS 2.8.5 API correctly for testing the DFSUtilClient.getHaNnWebHdfsAddresses method.
        String scheme = "webhdfs";
        Map<String, Map<String, InetSocketAddress>> result = DFSUtilClient.getHaNnWebHdfsAddresses(conf, scheme);

        // Validate the result and ensure no runtime errors occur.
        // Step 3.1: Validate the returned map and ensure it contains the expected data.
        assertNotNull("Result map is null", result);
        assertTrue("Result map does not contain the expected nameservice key", result.containsKey("nameservice1"));

        // Step 3.2: Verify the content of the map for "nameservice1".
        Map<String, InetSocketAddress> namenodeAddresses = result.get("nameservice1");
        assertNotNull("NameNode addresses map is null", namenodeAddresses);
        assertEquals("Unexpected number of NameNode addresses", 2, namenodeAddresses.size());

        // Step 3.3: Validate the correctness of the returned addresses.
        InetSocketAddress nn1Address = namenodeAddresses.get("nn1");
        InetSocketAddress nn2Address = namenodeAddresses.get("nn2");

        assertEquals("Unexpected address for nn1", NetUtils.createSocketAddr("localhost:50070"), nn1Address);
        assertEquals("Unexpected address for nn2", NetUtils.createSocketAddr("localhost:50071"), nn2Address);

        // Step 4: Test complete.
    }
}