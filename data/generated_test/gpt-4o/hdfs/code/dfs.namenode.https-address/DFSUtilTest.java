package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class DFSUtilTest {

    private Configuration configuration;
    private InetSocketAddress mockNamenodeAddr;

    @Before
    public void setUp() {
        // Prepare test conditions
        // 1. Instantiate configuration and setup a mock NameNode address.
        configuration = new Configuration();
        configuration.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "namenode.example.com:9871");
        mockNamenodeAddr = new InetSocketAddress("namenode.example.com", 9871);
    }

    @Test
    public void testHttpsSchemeWithValidSuffixes() throws Exception {
        // 2. Use the HDFS 2.8.5 API to test the "https" scheme.
        URI result = DFSUtil.getInfoServer(mockNamenodeAddr, configuration, "https");

        // 3. Test code: Verify the correctness of the result.
        assertEquals("https://namenode.example.com:9871", result.toString());
    }
}