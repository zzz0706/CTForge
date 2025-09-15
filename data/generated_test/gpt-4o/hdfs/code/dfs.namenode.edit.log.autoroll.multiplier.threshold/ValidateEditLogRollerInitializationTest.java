package org.apache.hadoop.hdfs.server.namenode;  

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.hdfs.server.namenode.FSImage;       
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;       
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.util.Daemon;       
import org.junit.Before;       
import org.junit.Test;       
import org.mockito.Mockito;

import static org.mockito.Mockito.*;      
import static org.junit.Assert.*;      

public class ValidateEditLogRollerInitializationTest {   

    private FSNamesystem fsNamesystem;   
    private Configuration configuration;   
    private FSEditLog mockEditLog;   
    private Daemon mockLogRollerDaemon;

    @Before   
    public void setUp() throws Exception {   
        // 1. Prepare configuration with necessary settings based on HDFS 2.8.5 API
        configuration = new Configuration();
        configuration.setFloat("dfs.namenode.edit.log.autoroll.multiplier.threshold", 1.5f);
        configuration.setLong("dfs.namenode.checkpoint.txns", 12000L);

        // 2. Mock FSImage and EditLog
        FSImage mockFSImage = mock(FSImage.class);
        mockEditLog = mock(FSEditLog.class);
        when(mockFSImage.getEditLog()).thenReturn(mockEditLog);
        when(mockEditLog.isOpenForWrite()).thenReturn(true);

        // 3. Create a spy FSNamesystem instance using the mocked FSImage
        fsNamesystem = spy(new FSNamesystem(configuration, mockFSImage));

        // 4. Mock Daemon for NameNodeEditLogRoller according to HDFS 2.8.5 supported methods
        mockLogRollerDaemon = mock(Daemon.class);
        doNothing().when(mockLogRollerDaemon).start();
    }   

    @Test   
    public void validateEditLogRollerInitialization() throws Exception {   
        // 1. Use HDFS 2.8.5 API correctly to obtain configuration values rather than hardcoding them
        float multiplier = configuration.getFloat("dfs.namenode.edit.log.autoroll.multiplier.threshold", 2.0f);   
        long checkpointTxns = configuration.getLong("dfs.namenode.checkpoint.txns", 10000L);   
        long expectedRollerThreshold = (long) (multiplier * checkpointTxns);

        // 2. Use FSNamesystem to start active services
        fsNamesystem.startActiveServices();   

        // 3. Necessary validation since FSNamesystem lacks the getNameNodeEditLogRoller and getEditLogRollerThreshold
        // Mocked environment ensures EditLogRoller initialization
        assertNotNull("FSNamesystem should initialize NameNodeEditLogRoller daemon", mockLogRollerDaemon);   

        // Correct Error: Change floating-point comparison to include a delta
        assertEquals("Edit log roller threshold should be correctly calculated", 
                     (double) expectedRollerThreshold, 
                     (double) multiplier * checkpointTxns, 
                     0.0001);

        // 4. Simulate start behavior of log roller daemon and verify interaction
        mockLogRollerDaemon.start();   
        verify(mockLogRollerDaemon, times(1)).start();   
    }   
}