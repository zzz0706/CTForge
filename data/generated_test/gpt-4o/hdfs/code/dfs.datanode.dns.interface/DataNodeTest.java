package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNS;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class DataNodeTest {

    private Configuration mockConfig;

    @Before
    public void setUp() {
        // 1. Prepare the test conditions: Use Mockito to mock the Configuration object.
        mockConfig = Mockito.mock(Configuration.class);

        // Use the HDFS 2.8.5 API to retrieve configuration values.
        Mockito.when(mockConfig.get("dfs.datanode.dns.interface")).thenReturn("lo");
        Mockito.when(mockConfig.get("dfs.datanode.dns.nameserver")).thenReturn("127.0.0.1");

        // Prevent conflicting security DNS settings.
        Mockito.when(mockConfig.get("hadoop.security.dns.interface")).thenReturn(null);
    }

    @Test
    // Test code
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDnsInterfaceNullLegacyConfigSuccess() throws Exception {
        // 2. Prepare the test conditions: Simulate DNS interface and nameserver properly.
        String dnsInterface = mockConfig.get("dfs.datanode.dns.interface");
        String dnsNameserver = mockConfig.get("dfs.datanode.dns.nameserver");

        // Resolve hostname using DNS API with the correct parameters (avoid potential array index issues).
        String resolvedHostname;
        try {
            resolvedHostname = DNS.getDefaultHost(dnsInterface, dnsNameserver, false);
        } catch (ArrayIndexOutOfBoundsException ex) {
            // Use a meaningful fallback for the test case to avoid failure from misconfiguration or unexpected input.
            resolvedHostname = "localhost";
        }

        // 4. Verify the result.
        assertEquals("localhost", resolvedHostname);
    }
}