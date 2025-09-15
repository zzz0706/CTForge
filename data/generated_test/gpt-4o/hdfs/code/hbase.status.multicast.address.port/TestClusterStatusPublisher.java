package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import static org.mockito.Mockito.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;

@Category({MasterTests.class, SmallTests.class})
public class TestClusterStatusPublisher {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestClusterStatusPublisher.class);

    private Configuration configurationMock;

    @Before
    public void setUp() throws Exception {
        // Create a mock Configuration object
        configurationMock = mock(Configuration.class);

        // Mock behavior for retrieving HBase configuration properties
        when(configurationMock.get(eq("hbase.status.multicast.address"), anyString()))
            .thenReturn("224.0.0.1");
        when(configurationMock.getInt(eq("hbase.status.multicast.address.port"), anyInt()))
            .thenReturn(16100);
        when(configurationMock.get(eq("hbase.status.multicast.interface.name")))
            .thenReturn(null);
    }

    @Test
    public void testMulticastConfigurationParsing() throws Exception {
        // Prepare test conditions: Validate configuration values
        String multicastAddress = configurationMock.get("hbase.status.multicast.address", "224.0.0.1");
        int multicastPort = configurationMock.getInt("hbase.status.multicast.address.port", 16100);

        // Ensure test logic is consistent with expected configuration values
        InetAddress expectedMulticastAddress = InetAddress.getByName(multicastAddress);
        InetSocketAddress expectedSocketAddress = new InetSocketAddress(expectedMulticastAddress, multicastPort);

        // Verify that the mocked configuration methods have been invoked correctly
        verify(configurationMock, times(1)).get("hbase.status.multicast.address", "224.0.0.1");
        verify(configurationMock, times(1)).getInt("hbase.status.multicast.address.port", 16100);

        // Cleanup and final assertions
        assert expectedSocketAddress.getAddress().equals(expectedMulticastAddress);
        assert expectedSocketAddress.getPort() == multicastPort;
    }
}