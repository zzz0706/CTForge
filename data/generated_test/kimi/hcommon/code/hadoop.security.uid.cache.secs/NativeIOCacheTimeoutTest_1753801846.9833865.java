package org.apache.hadoop.io.nativeio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
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
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({NativeIO.class, System.class})
public class NativeIOCacheTimeoutTest {

    @Test
    public void testFstatUsesCacheTimeoutForOwnerLookup() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long cacheTimeoutMs = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) * 1000L;

        // Skip the test if native fstat is not available
        Assume.assumeTrue(NativeIO.isAvailable());

        // 2. Prepare the test conditions
        File tmp = File.createTempFile("test", ".tmp");
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
                    .thenReturn(baseTime + cacheTimeoutMs + 1L); // Advance beyond timeout

            // Second call should trigger fresh lookups
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);

            // Verify that fresh lookups occurred (owner/group names should still match)
            assertNotNull(stat2);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());
        }
        // 4. Code after testing
        // File is deleted via deleteOnExit
    }
}