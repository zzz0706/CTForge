package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SecurityUtilConfigTest {

    private Level oldLevel;

    @Before
    public void setUp() throws Exception {
        // Ensure logging is on so we can capture slow-lookup warnings
        oldLevel = Logger.getLogger(SecurityUtil.class).getLevel();
        Logger.getLogger(SecurityUtil.class).setLevel(Level.WARN);
    }

    @After
    public void tearDown() throws Exception {
        Logger.getLogger(SecurityUtil.class).setLevel(oldLevel);
    }

    @Test
    public void testSlowLookupThresholdZeroAlwaysLogs() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                0); // zero means every lookup is considered slow

        // Force reload of slowLookupThresholdMs via reflection
        reloadSlowLookupThresholdMs(conf);

        String localhostName = InetAddress.getLocalHost().getHostName();

        // Capture logs
        org.apache.hadoop.test.GenericTestUtils.LogCapturer logs =
                org.apache.hadoop.test.GenericTestUtils.LogCapturer.captureLogs(Logger.getLogger(SecurityUtil.class));

        InetAddress result = SecurityUtil.getByName(localhostName);

        assertNotNull(result);
        assertTrue("Expected slow-lookup warning in logs",
                logs.getOutput().contains("Slow name lookup for"));
    }

    @Test
    public void testSlowLookupThresholdLargeNeverLogs() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                Integer.MAX_VALUE); // effectively disables slow-lookup logging

        reloadSlowLookupThresholdMs(conf);

        String localhostName = InetAddress.getLocalHost().getHostName();

        org.apache.hadoop.test.GenericTestUtils.LogCapturer logs =
                org.apache.hadoop.test.GenericTestUtils.LogCapturer.captureLogs(Logger.getLogger(SecurityUtil.class));

        InetAddress result = SecurityUtil.getByName(localhostName);

        assertNotNull(result);
        assertTrue("No slow-lookup warning expected",
                !logs.getOutput().contains("Slow name lookup for"));
    }

    @Test
    public void testDefaultThresholdIs1000() throws Exception {
        Configuration conf = new Configuration();
        assertTrue("Default threshold should be 1000",
                conf.getInt(
                        CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                        -1) == 1000);
    }

    /* helper to reset the private static field so tests see new conf */
    private void reloadSlowLookupThresholdMs(Configuration conf) throws Exception {
        Field confField = SecurityUtil.class.getDeclaredField("conf");
        confField.setAccessible(true);
        confField.set(null, conf);

        Field thresholdField = SecurityUtil.class.getDeclaredField("slowLookupThresholdMs");
        thresholdField.setAccessible(true);
        thresholdField.setInt(null, conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT));
    }
}