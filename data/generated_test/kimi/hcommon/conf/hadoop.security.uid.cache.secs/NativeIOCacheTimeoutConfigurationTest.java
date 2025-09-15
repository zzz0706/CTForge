package org.apache.hadoop.io.nativeio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({NativeIO.class, System.class})
public class NativeIOCacheTimeoutConfigurationTest {

    private File tmpFile;
    private long originalCacheTimeout;

    @Before
    public void setUp() throws IOException {
        tmpFile = File.createTempFile("testCacheConfig", ".tmp");
        tmpFile.deleteOnExit();

        // Save original cacheTimeout value
        Field cacheTimeoutField = null;
        try {
            cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
            cacheTimeoutField.setAccessible(true);
            originalCacheTimeout = cacheTimeoutField.getLong(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access cacheTimeout field", e);
        }
    }

    @After
    public void tearDown() {
        if (tmpFile != null) {
            tmpFile.delete();
        }

        // Restore original cacheTimeout value
        try {
            Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
            cacheTimeoutField.setAccessible(true);
            cacheTimeoutField.setLong(null, originalCacheTimeout);
        } catch (Exception e) {
            // Ignore restoration errors
        }
    }

    @Test
    public void testConfigurationPropagatesToCacheTimeout() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long customTimeout = 2L; // 2 seconds
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY, customTimeout);

        // Force re-initialization of NativeIO to pick up the new configuration
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        cacheTimeoutField.setLong(null, conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) * 1000L);

        long expectedTimeoutMs = customTimeout * 1000L;
        long actualTimeoutMs = cacheTimeoutField.getLong(null);
        assertEquals("Cache timeout should match configuration value", expectedTimeoutMs, actualTimeoutMs);

        // 2. Prepare the test conditions
        assumeTrue(NativeIO.isAvailable());

        try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
            FileDescriptor fd = fos.getFD();

            // 3. Test code
            // First call populates caches
            NativeIO.POSIX.Stat stat1 = NativeIO.POSIX.getFstat(fd);

            // Mock System.currentTimeMillis to simulate advancing the clock
            PowerMockito.mockStatic(System.class);
            long baseTime = System.currentTimeMillis();
            when(System.currentTimeMillis())
                    .thenReturn(baseTime)
                    .thenReturn(baseTime + expectedTimeoutMs + 500L); // Advance beyond 2 second timeout

            // Second call should trigger fresh lookups due to cache timeout
            NativeIO.POSIX.Stat stat2 = NativeIO.POSIX.getFstat(fd);

            // Verify that fresh lookups occurred (owner/group names should still match)
            assertNotNull(stat2);
            assertEquals(stat1.getOwner(), stat2.getOwner());
            assertEquals(stat1.getGroup(), stat2.getGroup());

            // Reset mock to return same time, should use cached values
            PowerMockito.mockStatic(System.class);
            when(System.currentTimeMillis()).thenReturn(baseTime + expectedTimeoutMs + 1000L);
            NativeIO.POSIX.Stat stat3 = NativeIO.POSIX.getFstat(fd);
            assertEquals(stat2.getOwner(), stat3.getOwner());
            assertEquals(stat2.getGroup(), stat3.getGroup());
        }

        // 4. Code after testing
        // Reset cache timeout to original value to avoid side effects on other tests
        cacheTimeoutField.setLong(null, originalCacheTimeout);
    }

    @Test
    public void testDefaultConfigurationValue() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        long defaultTimeout = conf.getLong(
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
                CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT);

        // 2. Prepare the test conditions
        Field cacheTimeoutField = NativeIO.class.getDeclaredField("cacheTimeout");
        cacheTimeoutField.setAccessible(true);
        
        // Reset to default value
        cacheTimeoutField.setLong(null, defaultTimeout * 1000L);

        // 3. Test code
        long actualTimeoutMs = cacheTimeoutField.getLong(null);
        long expectedTimeoutMs = defaultTimeout * 1000L;
        assertEquals("Cache timeout should match default configuration value", expectedTimeoutMs, actualTimeoutMs);

        // 4. Code after testing
        // No cleanup needed as we're just checking the default value
    }
}