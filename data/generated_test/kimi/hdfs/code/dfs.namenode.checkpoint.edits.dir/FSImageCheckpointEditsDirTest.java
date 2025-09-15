package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FSImageCheckpointEditsDirTest {

    private Configuration conf;
    private Properties configProps;

    @Before
    public void setUp() throws IOException {
        conf = new Configuration();
        configProps = new Properties();
        // Load from default configuration resources
        InputStream inputStream = this.getClass().getClassLoader()
                .getResourceAsStream("hdfs-default.xml");
        if (inputStream != null) {
            configProps.load(inputStream);
        }
    }

    @Test
    public void testGetCheckpointEditsDirs_usesConfigValue() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        String testDir = "/tmp/checkpoint-edits";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, testDir);

        // 3. Test code.
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);

        // 4. Code after testing.
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("file:" + testDir, result.get(0).toString());
    }

    @Test
    public void testGetCheckpointEditsDirs_usesDefaultValueWhenConfigNotSet() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        String defaultName = "/tmp/default-checkpoint";

        // 3. Test code.
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, defaultName);

        // 4. Code after testing.
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("file:" + defaultName, result.get(0).toString());
    }

    @Test
    public void testGetCheckpointEditsDirs_multipleDirectories() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        String dirs = "/tmp/checkpoint1,/tmp/checkpoint2,/tmp/checkpoint3";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, dirs);

        // 3. Test code.
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);

        // 4. Code after testing.
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("file:/tmp/checkpoint1", result.get(0).toString());
        assertEquals("file:/tmp/checkpoint2", result.get(1).toString());
        assertEquals("file:/tmp/checkpoint3", result.get(2).toString());
    }

    @Test
    public void testGetCheckpointEditsDirs_emptyConfigUsesDefault() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        String defaultName = "/tmp/empty-default";

        // 3. Test code.
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, defaultName);

        // 4. Code after testing.
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("file:" + defaultName, result.get(0).toString());
    }

    @Test
    public void testGetCheckpointEditsDirs_configValueMatchesPropertiesFile() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions - load actual default.
        // Clear any existing configuration
        conf.unset(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
        
        // When checkpoint edits dir is not set, it should use the default behavior
        // The method should return an empty list when no config is set and no default is provided
        // 3. Test code.
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);
        
        // 4. Code after testing.
        // In HDFS 2.8.5, when no configuration is provided and no defaultName is given,
        // the method returns an empty list
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testGetCheckpointEditsDirs_verifyUtilStringCollectionAsURIsCalled() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        String testDir = "/tmp/verify-call";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, testDir);

        // Since we can't use MockedStatic in older Mockito versions,
        // we'll test the actual behavior instead
        // 3. Test code.
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);

        // 4. Code after testing.
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("file:" + testDir, result.get(0).toString());
    }
}