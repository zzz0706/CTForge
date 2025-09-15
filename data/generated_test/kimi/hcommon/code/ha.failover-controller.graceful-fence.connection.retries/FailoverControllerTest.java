package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FailoverControllerTest {

    @Test
    public void testDefaultGracefulFenceRetriesAppliedToIPCConfig() throws Exception {
        // 1. Create a fresh Configuration instance without explicit set() calls
        Configuration conf = new Configuration();

        // 2. Compute expected value dynamically from default
        int expectedRetries = conf.getInt(
                CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES,
                CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES_DEFAULT);

        // 3. Instantiate FailoverController with the configuration
        FailoverController fc = new FailoverController(conf, null);

        // 4. Read the injected values from gracefulFenceConf via reflection
        java.lang.reflect.Field field = FailoverController.class.getDeclaredField("gracefulFenceConf");
        field.setAccessible(true);
        Configuration gracefulFenceConf = (Configuration) field.get(fc);

        int actualRetries = gracefulFenceConf.getInt(
                CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, -1);
        int actualTimeoutRetries = gracefulFenceConf.getInt(
                CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, -1);

        // 5. Assert both IPC keys are set to the expected default
        assertEquals(expectedRetries, actualRetries);
        assertEquals(expectedRetries, actualTimeoutRetries);
    }
}