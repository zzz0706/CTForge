package org.apache.hadoop.hdfs.server.namenode.top.window;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.TopWindow;
import org.apache.hadoop.metrics2.util.Metrics2Util.NameValuePair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({RollingWindowManager.class})
public class RollingWindowManagerConfigTest {

    @Mock
    private Configuration mockConf;

    @Mock
    private Iterator<Map.Entry<String, RollingWindow>> mockIterator;

    @Mock
    private Map.Entry<String, RollingWindow> mockEntry;

    @Mock
    private RollingWindow mockWindow;

    private RollingWindowManager rollingWindowManager;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDfsNamenodeTopNumUsers_ConfigValueIsUsedInConstructor() {
        // 1. Obtain configuration value using HDFS 2.8.5 API
        Configuration conf = new Configuration();
        int expectedTopUsersCnt = conf.getInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY,
                DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT);

        // Also verify against raw properties file loading
        Properties props = new Properties();
        try {
            props.load(this.getClass().getClassLoader().getResourceAsStream("hdfs-default.xml"));
            String propValue = props.getProperty(DFSConfigKeys.NNTOP_NUM_USERS_KEY);
            int propExpected = propValue != null ? Integer.parseInt(propValue) : 10;
            assertEquals("Property file and Configuration API should match", propExpected, expectedTopUsersCnt);
        } catch (Exception e) {
            // Fallback if resource not found
            assertEquals("Default value should be 10", 10, expectedTopUsersCnt);
        }

        // 2. Prepare test conditions
        when(mockConf.getInt(eq(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY),
                anyInt())).thenReturn(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_DEFAULT);
        when(mockConf.getInt(eq(DFSConfigKeys.NNTOP_NUM_USERS_KEY),
                anyInt())).thenReturn(expectedTopUsersCnt);

        // 3. Test code - create instance and verify field is set correctly
        rollingWindowManager = new RollingWindowManager(mockConf, 1000); // 1000ms reporting period

        // Verify the configuration was used correctly - expect multiple calls
        verify(mockConf, atLeastOnce()).getInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT);
        verify(mockConf, atLeastOnce()).getInt(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY, DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_DEFAULT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDfsNamenodeTopNumUsers_InvalidValue_ThrowsException() {
        // Test that invalid values trigger the expected validation error
        when(mockConf.getInt(eq(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_KEY),
                anyInt())).thenReturn(DFSConfigKeys.NNTOP_BUCKETS_PER_WINDOW_DEFAULT);
        when(mockConf.getInt(eq(DFSConfigKeys.NNTOP_NUM_USERS_KEY),
                anyInt())).thenReturn(0); // Invalid value

        // This should throw IllegalArgumentException due to precondition check
        new RollingWindowManager(mockConf, 1000);
    }

    @Test
    public void testDfsNamenodeTopNumUsers_DefaultValueUsedWhenNotSet() {
        // Test that default value is used when key is not present
        Configuration conf = new Configuration();
        conf.unset(DFSConfigKeys.NNTOP_NUM_USERS_KEY);
        int value = conf.getInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY,
                DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT);
        assertEquals("Default value should be used", 10, value);
    }
}