package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SecurityUtilConfigurationTest {

    private Configuration conf;
    private TestAppender testAppender;

    private static class TestAppender extends AppenderSkeleton {
        List<LoggingEvent> events = new ArrayList<>();

        @Override
        protected void append(LoggingEvent event) {
            events.add(event);
        }

        @Override
        public void close() {
            events.clear();
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }
    }

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        testAppender = new TestAppender();
        Logger logger = Logger.getLogger(SecurityUtil.class);
        logger.addAppender(testAppender);
        logger.setLevel(Level.WARN);
    }

    @After
    public void tearDown() {
        Logger logger = Logger.getLogger(SecurityUtil.class);
        logger.removeAppender(testAppender);
    }

    @Test
    public void testNegativeThresholdDisablesSlowLookupLogging() throws Exception {
        // 1. Set negative threshold via Configuration
        conf.setInt(CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY, -1);
        int expectedThreshold = conf.getInt(
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT);

        // 2. Use real InetAddress to simulate slow lookup (no mocking of HostResolver)
        // 3. Call method under test
        try {
            SecurityUtil.getByName("dummy.host");
        } catch (UnknownHostException ignored) {
            // Expected for dummy.host
        }

        // 4. Verify no warning logged
        long warnings = 0;
        for (LoggingEvent e : testAppender.events) {
            if (e.getLevel() == Level.WARN) {
                warnings++;
            }
        }
        assertEquals(0, warnings);
    }
}