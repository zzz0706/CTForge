package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class TestDFSUtil {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getInfoServer_https_scheme() throws IOException {
        // Step 1: Prepare the test conditions
        Configuration conf = new Configuration(); // Create a configuration object
        conf.set("dfs.http.policy", "HTTPS_ONLY"); // Set the HTTP policy to HTTPS_ONLY
        conf.set("dfs.namenode.https-address", "localhost:50470"); // Set the HTTPS address for the NameNode

        InetSocketAddress namenodeAddr = InetSocketAddress.createUnresolved("localhost", 8020); // Mock NameNode RPC Address

        // Step 2: Test code - Call getInfoServer() to validate HTTPS scheme
        URI infoServerURI = DFSUtil.getInfoServer(namenodeAddr, conf, "https");

        // Step 3: Validate the URI returned
        assertEquals("https://localhost:50470", infoServerURI.toString());

        // Step 4: Code after testing
        // No further cleanup required beyond the assertions
    }
}