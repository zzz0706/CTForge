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
import org.junit.Before;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({NativeIO.class, System.class})
public class NativeIOConfigurationCoverageTest {

    private File tmpFile;
    private long originalCacheTimeout;

    @Before
    public void setUp() throws Exception {
        // Skip the test if native fstat is not available
        Assume.assumeTrue(NativeIO.isAvailable());

        // Save the original cache timeout so we can restore it later
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        originalCacheTimeout = cacheTimeoutField.getLong(null);

        // Create a temporary file for testing
        tmpFile = File.createTempFile("nativeIOConfTest", ".tmp");
        tmpFile.deleteOnExit();

        // Clear any existing cache entries
        clearCache(NativeIO.class.getDeclaredField("USER_ID_NAME_CACHE"));
        clearCache(NativeIO.class.getDeclaredField("GROUP_ID_NAME_CACHE"));
    }

    @After
    public void tearDown() throws Exception {
        // Restore the original cache timeout
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null, originalCacheTimeout);
    }

    private void clearCache(Field cacheField) throws Exception {
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, ?> cache = (Map<Integer, ?>) cacheField.get(null);
        cache.clear();
    }

    @Test
    public void testConfigurationIsActuallyUsed() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long configuredCacheSecs = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT);
        long configuredCacheMs = configuredCacheSecs * 1000L;

        // 2. Prepare the test conditions
        // Force the cache timeout to the configured value
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null, configuredCacheMs);

        try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
            FileDescriptor fd = fos.getFD();

            // First call populates caches
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat1);
            assertNotNull(stat1.getOwner());
            assertNotNull(stat1.getGroup());

            // 3. Test code: Verify the configuration is actually used
            assertEquals("Configured cache timeout should be used",
                         configuredCacheMs, cacheTimeoutField.getLong(null));

            // Verify the cache is populated
            Field userCacheField = NativeIO.class.getDeclaredField("USER_ID_NAME_CACHE");
            userCacheField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<Integer, ?> userCache = (Map<Integer, ?>) userCacheField.get(null);
            assertFalse("User cache should contain entries after getFstat", userCache.isEmpty());

            Field groupCacheField = NativeIO.class.getDeclaredField("GROUP_ID_NAME_CACHE");
            groupCacheField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<Integer, ?> groupCache = (Map<Integer, ?>) groupCacheField.get(null);
            assertFalse("Group cache should contain entries after getFstat", groupCache.isEmpty());

            // 4. Code after testing: Verify cache expiry behavior
            long baseTime = System.currentTimeMillis();
            PowerMockito.mockStatic(System.class);
            // Simulate cache hit
            when(System.currentTimeMillis()).thenReturn(baseTime);
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertEquals("Cache hit should return same owner", stat1.getOwner(), stat2.getOwner());
            assertEquals("Cache hit should return same group", stat1.getGroup(), stat2.getGroup());

            // Simulate cache expiry
            when(System.currentTimeMillis()).thenReturn(baseTime + configuredCacheMs + 1);
            NativeIO.POSIX.Stat stat3 = NativeIO.POSIX.getFstat(fd);
            assertNotNull("Cache expiry should still return valid stat", stat3);
            assertEquals("Cache expiry should return same owner", stat1.getOwner(), stat3.getOwner());
            assertEquals("Cache expiry should return same group", stat1.getGroup(), stat3.getGroup());
        }
    }

    @Test
    public void testZeroCacheTimeoutFromConfiguration() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY, 0L);
        long configuredCacheSecs = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT);
        long configuredCacheMs = configuredCacheSecs * 1000L;

        // 2. Prepare the test conditions
        // Force the cache timeout to 0 (disabled)
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null, configuredCacheMs);

        try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
            FileDescriptor fd = fos.getFD();

            // 3. Test code: Verify caching is disabled
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat1);

            // Verify the cache timeout is 0
            assertEquals("Cache timeout should be 0 when disabled", 0L, cacheTimeoutField.getLong(null));

            // Verify the caches are not populated (since timeout is 0)
            Field userCacheField = NativeIO.class.getDeclaredField("USER_ID_NAME_CACHE");
            userCacheField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<Integer, ?> userCache = (Map<Integer, ?>) userCacheField.get(null);
            assertTrue("User cache should remain empty when disabled", userCache.isEmpty());

            Field groupCacheField = NativeIO.class.getDeclaredField("GROUP_ID_NAME_CACHE");
            groupCacheField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<Integer, ?> groupCache = (Map<Integer, ?>) groupCacheField.get(null);
            assertTrue("Group cache should remain empty when disabled", groupCache.isEmpty());

            // 4. Code after testing: Verify fresh lookups occur every time
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat2);
            assertEquals("Fresh lookup should return same owner", stat1.getOwner(), stat2.getOwner());
            assertEquals("Fresh lookup should return same group", stat1.getGroup(), stat2.getGroup());
        }
    }
}