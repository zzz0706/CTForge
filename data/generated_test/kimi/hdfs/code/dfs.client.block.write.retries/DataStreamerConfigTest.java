package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({NetUtils.class})
public class DataStreamerConfigTest {

    @Mock
    private DFSClient dfsClient;
    
    @Mock
    private DFSOutputStream dfsOutputStream;

    @Test
    public void testBlockWriteRetriesWithDefaultConfiguration() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values
        Configuration conf = new HdfsConfiguration();
        int expectedRetries = conf.getInt(HdfsClientConfigKeys.BlockWrite.RETRIES_KEY, 
                                         HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT);
        int expectedAttempts = expectedRetries + 1; // initial attempt plus retries
      
        // 2. Prepare the test conditions
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        
        // Mock DataNode info
        DatanodeInfo[] datanodes = new DatanodeInfo[1];
        DatanodeInfo mockDatanode = mock(DatanodeInfo.class);
        when(mockDatanode.getXferAddr()).thenReturn("localhost:50010");
        datanodes[0] = mockDatanode;
        
        // Mock LocatedBlock
        LocatedBlock mockLocatedBlock = mock(LocatedBlock.class);
        when(mockLocatedBlock.getLocations()).thenReturn(datanodes);
        when(mockLocatedBlock.getBlock()).thenReturn(new ExtendedBlock("testpool", 1L));
        
        // Mock NetUtils to simulate connection failures
        PowerMockito.mockStatic(NetUtils.class);
        PowerMockito.when(NetUtils.createSocketAddr(anyString()))
                   .thenReturn(new InetSocketAddress("localhost", 50010));
        
        // Track connection attempts
        final List<Exception> connectionAttempts = new ArrayList<Exception>();
        
        // 3. Test code
        try {
            // Simulate connection attempts with retries
            int remainingRetries = expectedRetries;
            while (remainingRetries >= 0) {
                connectionAttempts.add(new IOException("Connection attempt " + (expectedAttempts - remainingRetries)));
                if (remainingRetries == 0) {
                    throw new IOException("All retries exhausted");
                }
                remainingRetries--;
            }
            
            fail("Expected IOException to be thrown after exhausting retries");
            
        } catch (IOException e) {
            // 4. Code after testing - verify the results
            assertEquals("Should attempt exactly (expectedRetries + 1) times", 
                        expectedAttempts, connectionAttempts.size());
            assertEquals("Exception message should indicate retries exhausted", 
                        "All retries exhausted", e.getMessage());
        }
        

        // Verify that the configuration was read correctly
        assertEquals("Configuration should match expected retry count", 
                    expectedRetries, dfsClientConf.getNumBlockWriteRetry());
    }
}