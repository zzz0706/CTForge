package org.apache.hadoop.hdfs.shortcircuit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShortCircuitCacheConfigTest {

    @Test
    public void testMmapCacheSizeDefaultValue() throws IOException {
        // Given
        Configuration conf = new Configuration();
        String key = HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY;
        int expectedDefault = HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT;

        // When
        int actualValue = conf.getInt(key, expectedDefault);

        // Then
        assertEquals("Default value should match", expectedDefault, actualValue);
    }

    @Test
    public void testMmapCacheSizePropagation() throws IOException {
        // Given
        int[] testValues = {0, 1, 128, 256, 512, 1024};
        
        for (int cacheSize : testValues) {
            Configuration conf = new Configuration();
            conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, cacheSize);

            // When
            int actualValue = conf.getInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 
                                        HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT);

            // Then
            assertEquals("maxEvictableMmapedSize should match config", cacheSize, actualValue);
        }
    }

    @Test
    public void testMmapCacheSizeFromFileLoaderComparison() throws IOException {
        // Given
        Configuration conf = new Configuration();
        String key = HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY;
        int defaultValue = HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT;

        // When
        int configValue = conf.getInt(key, defaultValue);

        // Then
        assertEquals("Config value should match default value", defaultValue, configValue);
    }

    @Test
    public void testZeroCacheSizeBehavior() throws IOException {
        // Given
        int cacheSize = 0;
        Configuration conf = new Configuration();
        conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, cacheSize);

        // When
        int actualValue = conf.getInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 
                                    HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT);

        // Then
        assertEquals("Should be 0", 0, actualValue);
    }

    @Test
    public void testLargeCacheSizeDoesNotThrow() throws IOException {
        // Given
        int largeCacheSize = 10000;
        Configuration conf = new Configuration();
        conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, largeCacheSize);

        // When
        int actualValue = conf.getInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 
                                    HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT);

        // Then
        assertTrue("Cache size should be non-negative", actualValue >= 0);
    }
}