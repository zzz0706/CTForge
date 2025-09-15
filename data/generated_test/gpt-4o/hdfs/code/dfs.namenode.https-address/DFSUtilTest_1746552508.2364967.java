package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class DFSUtilTest {

    private Configuration mockConf;

    @Before
    public void setUp() {
        // 1. Prepare mock Configuration to simulate environment.
        mockConf = mock(Configuration.class);
        when(mockConf.get(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY))
                .thenReturn(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT);
    }

    @Test
    public void testHttpsSchemeWithNoSuffixes() throws Exception {
        // 2. Call getInfoServer with scheme='https' and null namenodeAddr.
        URI result = DFSUtil.getInfoServer(null, mockConf, "https");

        // 3. Assert the expected result using default HTTPS address from Configuration.
        String expectedUri = "https://" + mockConf.get(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
        assertEquals("Returned URI should match the default HTTPS address", expectedUri, result.toString());
    }
}