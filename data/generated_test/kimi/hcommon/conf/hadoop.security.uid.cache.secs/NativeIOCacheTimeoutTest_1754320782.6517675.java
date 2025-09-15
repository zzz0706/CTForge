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
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({NativeIO.class, System.class})
public class NativeIOCacheTimeoutTest {

    @Test
    public void testCacheExpiryTriggersFreshLookup() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long configuredCacheSecs = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT);
        long configuredCacheMs = configuredCacheSecs * 1000L;

        // Skip the test if native fstat is not available
        Assume.assumeTrue(NativeIO.isAvailable());

        // 2. Prepare the test conditions
        // Create a temporary file
        File tmp = File.createTempFile("test", ".tmp");
        tmp.deleteOnExit();

        // Force the cache timeout to 1 second to speed up the test
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null, 1000L);

        // Clear any existing cache entries
        Field userCacheField = NativeIO.class.getDeclaredField("USER_ID_NAME_CACHE");
        userCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, ?> userCache = (Map<Integer, ?>) userCacheField.get(null);
        userCache.clear();

        Field groupCacheField = NativeIO.class.getDeclaredField("GROUP_ID_NAME_CACHE");
        groupCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, ?> groupCache = (Map<Integer, ?>) groupCacheField.get(null);
        groupCache.clear();

        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            FileDescriptor fd = fos.getFD();

            // First call populates caches
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat1);
            assertNotNull(stat1.getOwner());
            assertNotNull(stat1.getGroup());

            // 3. Test code: Mock System.currentTimeMillis to simulate expiry
            long baseTime = System.currentTimeMillis();
            PowerMockito.mockStatic(System.class);
            // First call (cache hit)
            when(System.currentTimeMillis()).thenReturn(baseTime);
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());

            // Second call (cache expiry)
            when(System.currentTimeMillis()).thenReturn(baseTime + 1001L); // Advance by 1001ms
            NativeIO.POSIX.Stat stat3 = NativeIO.POSIX.getFstat(fd);

            // 4. Verify that fresh lookups occurred
            assertNotNull(stat3);
            assertEquals(stat1.getOwner(), stat3.getOwner());
            assertEquals(stat1.getGroup(), stat3.getGroup());

            // Verify that the cache timeout was actually used
            assertEquals(1000L, cacheTimeoutField.getLong(null));
            assertEquals(configuredCacheSecs * 1000L, configuredCacheMs);
        }
    }

    @Test
    public void testZeroCacheTimeoutDisablesCaching() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long configuredCacheSecs = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT);

        // Skip the test if native fstat is not available
        Assume.assumeTrue(NativeIO.isAvailable());

        // 2. Prepare the test conditions
        File tmp = File.createTempFile("testZero", ".tmp");
        tmp.deleteOnExit();

        // Set cache timeout to 0 to disable caching
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null, 0L);

        // Clear caches
        Field userCacheField = NativeIO.class.getDeclaredField("USER_ID_NAME_CACHE");
        userCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, ?> userCache = (Map<Integer, ?>) userCacheField.get(null);
        userCache.clear();

        Field groupCacheField = NativeIO.class.getDeclaredField("GROUP_ID_NAME_CACHE");
        groupCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, ?> groupCache = (Map<Integer, ?>) groupCacheField.get(null);
        groupCache.clear();

        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            FileDescriptor fd = fos.getFD();

            // 3. Test code: Multiple calls should always trigger fresh lookups
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat1);

            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat2);

            // 4. Verify that the cache timeout was 0
            assertEquals(0L, cacheTimeoutField.getLong(null));
            assertTrue(configuredCacheSecs > 0); // Ensure default is non-zero
        }
    }
}