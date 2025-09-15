package org.apache.hadoop.io.nativeio;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({NativeIO.class, System.class})
public class NativeIOConfigurationCoverageTest {

    private static final String CACHE_TIMEOUT_FIELD = "cacheTimeout";

    private long originalCacheTimeout;

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue(NativeIO.isAvailable());
        // Save original cache timeout
        Field cacheTimeoutField = NativeIO.class.getDeclaredField(CACHE_TIMEOUT_FIELD);
        cacheTimeoutField.setAccessible(true);
        originalCacheTimeout = cacheTimeoutField.getLong(null);
    }

    @After
    public void tearDown() throws Exception {
        // Restore original cache timeout
        Field cacheTimeoutField = NativeIO.class.getDeclaredField(CACHE_TIMEOUT_FIELD);
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null, originalCacheTimeout);
    }

    private void setCacheTimeout(long timeoutMs) throws Exception {
        Field cacheTimeoutField = NativeIO.class.getDeclaredField(CACHE_TIMEOUT_FIELD);
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null, timeoutMs);
    }

    @Test
    public void testConfigurationParsingAndUsage() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        long configuredTimeout = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) * 1000L;

        // 2. Prepare the test conditions.
        setCacheTimeout(configuredTimeout);
        File tmp = File.createTempFile("testConfig", ".tmp");
        tmp.deleteOnExit();

        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            FileDescriptor fd = fos.getFD();

            // 3. Test code.
            // First call populates cache
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat1);
            assertNotNull(stat1.getOwner());
            assertNotNull(stat1.getGroup());

            // Mock time to simulate cache hit
            PowerMockito.mockStatic(System.class);
            when(System.currentTimeMillis()).thenReturn(configuredTimeout / 2);
            
            // Second call should use cache
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());
        }
        // 4. Code after testing.
    }

    @Test
    public void testZeroTimeoutConfiguration() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY, 0L);
        long configuredTimeout = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) * 1000L;

        // 2. Prepare the test conditions.
        setCacheTimeout(configuredTimeout);
        
        File tmp = File.createTempFile("testZero", ".tmp");
        tmp.deleteOnExit();

        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            FileDescriptor fd = fos.getFD();

            // 3. Test code.
            // First call
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat1);

            // Second call should always refresh due to zero timeout
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat2);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());
        }
        // 4. Code after testing.
    }

    @Test
    public void testLargeTimeoutConfiguration() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY, 3600L); // 1 hour
        long configuredTimeout = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) * 1000L;

        // 2. Prepare the test conditions.
        setCacheTimeout(configuredTimeout);
        
        File tmp = File.createTempFile("testLarge", ".tmp");
        tmp.deleteOnExit();

        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            FileDescriptor fd = fos.getFD();

            // 3. Test code.
            // First call
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat1);

            // Mock time to simulate cache hit even after long time
            PowerMockito.mockStatic(System.class);
            when(System.currentTimeMillis())
                .thenReturn(0L)
                .thenReturn(configuredTimeout / 2);
            
            // Second call should still use cache
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());
        }
        // 4. Code after testing.
    }

    @Test
    public void testCacheRefreshAfterTimeout() throws Exception {
        // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY, 2L); // 2 seconds
        long configuredTimeout = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) * 1000L;

        // 2. Prepare the test conditions.
        setCacheTimeout(configuredTimeout);
        
        File tmp = File.createTempFile("testRefresh", ".tmp");
        tmp.deleteOnExit();

        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            FileDescriptor fd = fos.getFD();

            // 3. Test code.
            // First call populates cache
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat1);

            // Mock time progression
            PowerMockito.mockStatic(System.class);
            long baseTime = 1000000L;
            when(System.currentTimeMillis())
                .thenReturn(baseTime)                    // Initial call
                .thenReturn(baseTime + configuredTimeout / 2) // Within timeout
                .thenReturn(baseTime + configuredTimeout + 1000); // After timeout

            // Second call within timeout
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());

            // Third call after timeout should refresh
            NativeIO.POSIX.Stat stat3 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat3);
            assertEquals(stat1.getOwner(), stat3.getOwner());
            assertEquals(stat1.getGroup(), stat3.getGroup());
        }
        // 4. Code after testing.
    }
}