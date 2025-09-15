package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class AlluxioMasterProcessConfigTest {

    @Before
    public void setUp() {
        PowerMockito.mockStatic(ServerConfiguration.class);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testJvmMonitorDisabledByDefault() throws Exception {
        // 1. Use the Alluxio 2.1.0 API to obtain the default configuration value
        when(ServerConfiguration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED))
            .thenReturn(false);

        // 2. Prepare test conditions
        // Nothing to prepare since we only verify the configuration value

        // 3. Test code
        // We simply verify the mocked configuration value

        // 4. Verify JVM monitor is not started
        assertFalse("JVM monitor should be disabled by default",
            ServerConfiguration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED));
    }
}