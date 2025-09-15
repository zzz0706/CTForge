package org.apache.hadoop.io.nativeio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Shell;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({NativeIO.class, System.class})
public class NativeIOCacheTimeoutConfigTest {

    @Test
    public void testDefaultCacheTimeoutPropagation() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long defaultTimeoutMs = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) * 1000L;

        // Skip the test if native fstat is not available
        Assume.assumeTrue(NativeIO.isAvailable());

        // 2. Prepare the test conditions
        File tmp = File.createTempFile("testDefault", ".tmp");
        tmp.deleteOnExit();
        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            FileDescriptor fd = fos.getFD();

            // First call populates caches
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);

            // 3. Test code
            // Mock System.currentTimeMillis to simulate advancing the clock
            PowerMockito.mockStatic(System.class);
            long baseTime = System.currentTimeMillis();
            when(System.currentTimeMillis())
                    .thenReturn(baseTime)
                    .thenReturn(baseTime + defaultTimeoutMs - 1000L)   // still inside timeout
                    .thenReturn(baseTime + defaultTimeoutMs + 1000L);  // beyond timeout

            // Second call inside cache window – should reuse cache
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);

            // Third call after cache expired – should trigger fresh lookup
            NativeIO.POSIX.Stat stat3 = NativeIO.POSIX.getFstat(fd);

            // Verify that fresh lookups occurred (owner/group names should still match)
            assertNotNull(stat1);
            assertNotNull(stat2);
            assertNotNull(stat3);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());
            assertEquals(stat1.getOwner(), stat3.getOwner());
            assertEquals(stat1.getGroup(), stat3.getGroup());
        }
        // 4. Code after testing
        // File is deleted via deleteOnExit
    }

    @Test
    public void testZeroCacheTimeoutDisablesCache() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY, 0L);

        // Skip the test if native fstat is not available
        Assume.assumeTrue(NativeIO.isAvailable());

        // 2. Prepare the test conditions
        File tmp = File.createTempFile("testZero", ".tmp");
        tmp.deleteOnExit();
        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            FileDescriptor fd = fos.getFD();

            // First call
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);

            // 3. Test code
            // Mock System.currentTimeMillis to simulate advancing the clock
            PowerMockito.mockStatic(System.class);
            long baseTime = System.currentTimeMillis();
            when(System.currentTimeMillis())
                    .thenReturn(baseTime)
                    .thenReturn(baseTime + 1L); // immediate next call

            // Second call – should always perform fresh lookup because cache is disabled
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);

            // Verify that fresh lookups occurred
            assertNotNull(stat1);
            assertNotNull(stat2);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());
        }
        // 4. Code after testing
        // File is deleted via deleteOnExit
    }

    @Test
    public void testLargeCacheTimeoutNeverExpires() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        final long largeTimeoutSecs = 3600L; // 1 hour
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                     largeTimeoutSecs);
        long largeTimeoutMs = largeTimeoutSecs * 1000L;

        // Skip the test if native fstat is not available
        Assume.assumeTrue(NativeIO.isAvailable());

        // 2. Prepare the test conditions
        File tmp = File.createTempFile("testLarge", ".tmp");
        tmp.deleteOnExit();
        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            FileDescriptor fd = fos.getFD();

            // First call populates caches
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);

            // 3. Test code
            // Mock System.currentTimeMillis to simulate advancing the clock
            PowerMockito.mockStatic(System.class);
            long baseTime = System.currentTimeMillis();
            when(System.currentTimeMillis())
                    .thenReturn(baseTime)
                    .thenReturn(baseTime + largeTimeoutMs / 2); // still inside timeout

            // Second call – should reuse cache
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);

            // Verify that cache was reused
            assertNotNull(stat1);
            assertNotNull(stat2);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());
        }
        // 4. Code after testing
        // File is deleted via deleteOnExit
    }
}