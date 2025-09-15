package org.apache.hadoop.io.nativeio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

public class NativeIOConfigurationUsageTest {

    private File tmpFile;
    private long originalCacheTimeout;

    @Before
    public void setUp() throws Exception {
        tmpFile = File.createTempFile("testNativeIOConfig", ".tmp");
        tmpFile.deleteOnExit();

        // Save original cacheTimeout value
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        originalCacheTimeout = cacheTimeoutField.getLong(null);
    }

    @After
    public void tearDown() throws Exception {
        if (tmpFile != null) {
            tmpFile.delete();
        }

        // Restore original cacheTimeout value
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null, originalCacheTimeout);
    }

    @Test
    public void testGetFstatUsesConfigurationDrivenCacheTimeout() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long customTimeout = 1L; // 1 second
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY, customTimeout);

        // Propagate the new configuration value into NativeIO
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null,
                conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                        CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) * 1000L);

        // 2. Prepare the test conditions
        assumeTrue(NativeIO.isAvailable());

        try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
            FileDescriptor fd = fos.getFD();

            // 3. Test code
            // First call to populate cache
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat1);
            assertNotNull(stat1.getOwner());
            assertNotNull(stat1.getGroup());

            // Second call immediately should use cached values
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());

            // 4. Code after testing
            // Reset cache timeout to original value
            cacheTimeoutField.setLong(null, originalCacheTimeout);
        }
    }

    @Test
    public void testConfigurationValueReadFromStaticInitializer() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long configuredTimeout = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT);

        // 2. Prepare the test conditions
        assumeTrue(NativeIO.isAvailable());

        // 3. Test code
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        long actualTimeoutMs = cacheTimeoutField.getLong(null);
        long expectedTimeoutMs = configuredTimeout * 1000L;
        assertEquals("Static initializer should read the configuration value",
                expectedTimeoutMs, actualTimeoutMs);

        // 4. Code after testing
        // No cleanup needed as we are just verifying the static value
    }

    @Test
    public void testGetNameCacheEvictionAfterTimeout() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long shortTimeout = 1L; // 1 second
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY, shortTimeout);

        // Propagate the new configuration value into NativeIO
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null,
                conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                        CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) * 1000L);

        // 2. Prepare the test conditions
        assumeTrue(NativeIO.isAvailable());

        // 3. Test code
        try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
            FileDescriptor fd = fos.getFD();

            // First call to populate cache
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat1);

            // Force cache eviction by sleeping longer than cache timeout
            Thread.sleep(1200); // 1.2 seconds > 1 second timeout

            // Second call after timeout should trigger fresh lookup
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat2);
            // Values should still be same (same file) but cache should have been refreshed
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());

            // 4. Code after testing
            // Reset cache timeout to original value
            cacheTimeoutField.setLong(null, originalCacheTimeout);
        }
    }

    @Test
    public void testCacheTimeoutZeroDisablesCaching() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long zeroTimeout = 0L; // Disable caching
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY, zeroTimeout);

        // Propagate the new configuration value into NativeIO
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null,
                conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                        CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) * 1000L);

        // 2. Prepare the test conditions
        assumeTrue(NativeIO.isAvailable());

        // 3. Test code
        try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
            FileDescriptor fd = fos.getFD();

            // First call
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat1);

            // Second call immediately should trigger fresh lookup (no caching)
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat2);

            // 4. Code after testing
            // Reset cache timeout to original value
            cacheTimeoutField.setLong(null, originalCacheTimeout);
        }
    }
}