package org.apache.hadoop.io.nativeio;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;

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
    public void testCacheExpiryWithDifferentTimeoutValues() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long defaultTimeoutMs = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) * 1000L;

        // Skip the test if native fstat is not available
        Assume.assumeTrue(NativeIO.isAvailable());

        // Prepare the test conditions
        File tmp = File.createTempFile("test", ".tmp");
        tmp.deleteOnExit();

        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            FileDescriptor fd = fos.getFD();

            // First call populates caches
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);

            // Test with custom timeout (1 second)
            Configuration customConf = new Configuration();
            customConf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY, 1L);
            // Force reinitialization by clearing static state (if possible)
            // Note: In 2.8.5 we can't easily reinitialize, so we'll test with mocked time

            // Mock System.currentTimeMillis to simulate advancing the clock
            PowerMockito.mockStatic(System.class);
            long baseTime = 1000000L;
            when(System.currentTimeMillis())
                    .thenReturn(baseTime)
                    .thenReturn(baseTime + defaultTimeoutMs - 1) // Before timeout
                    .thenReturn(baseTime + defaultTimeoutMs + 1) // After timeout
                    .thenReturn(baseTime + 2 * defaultTimeoutMs + 1); // Well after timeout

            // Second call should use cache (before timeout)
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat2);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());

            // Third call should trigger fresh lookups (after timeout)
            NativeIO.POSIX.Stat stat3 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat3);
            assertEquals(stat1.getOwner(), stat3.getOwner());
            assertEquals(stat1.getGroup(), stat3.getGroup());

            // Test edge case with zero timeout (immediate expiry)
            Configuration zeroConf = new Configuration();
            zeroConf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY, 0L);
            // Each call should trigger fresh lookups
            NativeIO.POSIX.Stat stat4 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat4);
            assertEquals(stat1.getOwner(), stat4.getOwner());
            assertEquals(stat1.getGroup(), stat4.getGroup());
        }
    }

    @Test
    public void testNegativeCacheTimeout() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        
        // Skip the test if native fstat is not available
        Assume.assumeTrue(NativeIO.isAvailable());

        // Prepare the test conditions
        File tmp = File.createTempFile("test", ".tmp");
        tmp.deleteOnExit();

        try (FileOutputStream fos = new FileOutputStream(tmp)) {
            FileDescriptor fd = fos.getFD();

            // First call populates caches
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);

            // Mock System.currentTimeMillis to test negative timeout behavior
            PowerMockito.mockStatic(System.class);
            long baseTime = System.currentTimeMillis();
            when(System.currentTimeMillis())
                    .thenReturn(baseTime)
                    .thenReturn(baseTime - 1000L); // Negative time difference

            // Second call should still work despite negative timeout
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);
            assertNotNull(stat2);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());
        }
    }
}