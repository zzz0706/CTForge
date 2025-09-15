package org.apache.hadoop.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.HealthMonitor;
import org.junit.Test;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * Test to verify that HealthMonitor loopUntilConnected uses the configuration effect correctly.
 */
public class TestHealthMonitorConfigurationEffect {

    @Test
    public void testLoopUntilConnected_RetriesUntilConnectionSucceeds_RespectsConfiguredTimeout() throws Exception {
        Configuration conf = new Configuration();
        final int retriesToSucceed = 3;
        // Use the correct config key strings because HealthMonitor does NOT define public static final fields for the config keys in 2.8.x
        conf.setLong("ha.health-monitor.connect-retry-interval.ms", 10L); // Minimal retry wait

        // get value with API in test body
        final int rpcTimeoutFromConf = conf.getInt(
                "ha.health-monitor.rpc-timeout.ms",
                45000);

        // Set up a mock target that fails 'retriesToSucceed-1' times before succeeding
        final HAServiceTarget target = mock(HAServiceTarget.class);
        final AtomicInteger attempt = new AtomicInteger();
        final HAServiceProtocol protocolProxy = mock(HAServiceProtocol.class);

        // Use Answer with anonymous inner class to support Java 7
        when(target.getHealthMonitorProxy(any(Configuration.class), anyInt()))
                .thenAnswer(new org.mockito.stubbing.Answer<HAServiceProtocol>() {
                    @Override
                    public HAServiceProtocol answer(org.mockito.invocation.InvocationOnMock invocation) throws Throwable {
                        int currentAttempt = attempt.getAndIncrement();
                        Integer providedTimeout = (Integer) invocation.getArguments()[1];
                        // All calls use the config-provided value
                        assertTrue(providedTimeout == rpcTimeoutFromConf);

                        if (currentAttempt < retriesToSucceed - 1) {
                            throw new IOException("Simulated connection failure [" + currentAttempt + "]");
                        } else {
                            return protocolProxy;
                        }
                    }
                });

        HealthMonitor monitor = new HealthMonitor(conf, target);

        // Call the method-under-test via reflection as it is private in Hadoop 2.8.5
        java.lang.reflect.Method m = HealthMonitor.class.getDeclaredMethod("loopUntilConnected");
        m.setAccessible(true);
        m.invoke(monitor);

        // Ensure it called the proxy exactly the expected number of times
        verify(target, times(retriesToSucceed)).getHealthMonitorProxy(any(Configuration.class), anyInt());
        // Also, check the monitor's internal proxy field is actually set (connection succeeded)
        assertTrue(TestUtils.getField(monitor, "proxy") != null);
    }

    // TestUtils utility: get 'private' field via reflection for assertion (for demonstration)
    static class TestUtils {
        static Object getField(Object obj, String fieldName) throws Exception {
            java.lang.reflect.Field f = obj.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);
            return f.get(obj);
        }
    }
}