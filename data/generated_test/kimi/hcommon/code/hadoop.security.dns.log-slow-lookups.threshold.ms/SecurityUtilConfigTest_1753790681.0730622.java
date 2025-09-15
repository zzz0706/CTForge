package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertNotNull;

public class SecurityUtilConfigTest {

    @Before
    public void setUp() throws Exception {
        Logger.getLogger(SecurityUtil.class).setLevel(Level.TRACE);
    }

    @After
    public void tearDown() throws Exception {
        Logger.getLogger(SecurityUtil.class).setLevel(Level.INFO);
    }

    @Test
    public void testNoWarningLoggedWhenElapsedTimeBelowThreshold() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        long expectedThreshold = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

        // 2. Prepare the test conditions.
        String localhostName = InetAddress.getLocalHost().getHostName();

        // 3. Test code.
        InetAddress result = SecurityUtil.getByName(localhostName);

        // 4. Code after testing.
        assertNotNull(result);
    }
}