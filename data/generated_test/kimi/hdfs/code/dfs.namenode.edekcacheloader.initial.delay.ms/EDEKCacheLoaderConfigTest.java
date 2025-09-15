package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import java.io.IOException;

public class EDEKCacheLoaderConfigTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testEDEKCacheLoaderInitialDelayConfigValue() {
        // Given: a configuration with the target property set
        int expectedDelay = 5000;
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY, expectedDelay);

        // When: FSNamesystem initializes and reads the configuration
        FSImage fsImage = mock(FSImage.class);
        FSNamesystem realFsn = null;
        try {
            realFsn = new FSNamesystem(conf, fsImage);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create FSNamesystem", e);
        }
        
        // Use reflection to access private field
        int actualDelay = 0;
        try {
            java.lang.reflect.Field field = FSNamesystem.class.getDeclaredField("edekCacheLoaderDelay");
            field.setAccessible(true);
            actualDelay = field.getInt(realFsn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Then: the value should match what was configured
        assertEquals("EDEK cache loader initial delay should match configured value",
                expectedDelay, actualDelay);
    }

    @Test
    public void testEDEKCacheLoaderInitialDelayDefaultValue() {
        // Given: no explicit configuration for the property
        
        // When: FSNamesystem initializes with default configuration
        FSImage fsImage = mock(FSImage.class);
        FSNamesystem realFsn = null;
        try {
            realFsn = new FSNamesystem(conf, fsImage);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create FSNamesystem", e);
        }
        
        // Use reflection to access private field
        int actualDelay = 0;
        try {
            java.lang.reflect.Field field = FSNamesystem.class.getDeclaredField("edekCacheLoaderDelay");
            field.setAccessible(true);
            actualDelay = field.getInt(realFsn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Then: the value should match the default defined in DFSConfigKeys
        int defaultDelay = DFSConfigKeys.DFS_NAMENODE_EDEKCACHELOADER_INITIAL_DELAY_MS_DEFAULT;
        assertEquals("EDEK cache loader initial delay should match default value",
                defaultDelay, actualDelay);
    }
}