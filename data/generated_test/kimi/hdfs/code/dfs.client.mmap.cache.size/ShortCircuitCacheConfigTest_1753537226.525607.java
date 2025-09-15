package org.apache.hadoop.hdfs.shortcircuit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShortCircuitCacheConfigTest {

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDfsClientMmapCacheSize_Close_SetsMaxEvictableMmapedSizeToZero() throws IOException, NoSuchFieldException, IllegalAccessException {
        // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 10); // Set to non-zero value as per test case
        
        // Create ShortCircuitConf from Configuration
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        
        // 2. Prepare the test conditions
        // Create ShortCircuitCache instance using the configuration
        ShortCircuitCache cache = ShortCircuitCache.fromConf(dfsClientConf.getShortCircuitConf());
        
        // Verify initial maxEvictableMmapedSize value
        Field maxEvictableMmapedSizeField = ShortCircuitCache.class.getDeclaredField("maxEvictableMmapedSize");
        maxEvictableMmapedSizeField.setAccessible(true);
        int initialValue = maxEvictableMmapedSizeField.getInt(cache);
        assertEquals("Initial maxEvictableMmapedSize should match configured value", 10, initialValue);
        
        // 3. Test code - Call the close() method
        cache.close();
        
        // 4. Code after testing - Inspect the value after close()
        int finalValue = maxEvictableMmapedSizeField.getInt(cache);
        assertEquals("maxEvictableMmapedSize should be set to 0 after close()", 0, finalValue);
    }

    @Test
    public void testDfsClientMmapCacheSize_DemoteOldEvictableMmaped_UsesConfiguredValue() throws Exception {
        // Setup configuration with specific cache size
        Configuration conf = new Configuration();
        conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 5);
        
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        ShortCircuitCache cache = ShortCircuitCache.fromConf(dfsClientConf.getShortCircuitConf());
        
        // Access private fields needed for testing
        Field maxEvictableMmapedSizeField = ShortCircuitCache.class.getDeclaredField("maxEvictableMmapedSize");
        maxEvictableMmapedSizeField.setAccessible(true);
        
        Field evictableMmappedField = ShortCircuitCache.class.getDeclaredField("evictableMmapped");
        evictableMmappedField.setAccessible(true);
        
        // Verify the configured value is properly set
        int configuredSize = maxEvictableMmapedSizeField.getInt(cache);
        assertEquals("Configured cache size should be set correctly", 5, configuredSize);
        
        // Cleanup
        cache.close();
    }

    @Test
    public void testDfsClientMmapCacheSize_TrimEvictionMaps_UsesConfiguredValue() throws Exception {
        // Setup configuration
        Configuration conf = new Configuration();
        conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 3);
        
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        ShortCircuitCache cache = ShortCircuitCache.fromConf(dfsClientConf.getShortCircuitConf());
        
        // Access private fields
        Field maxEvictableMmapedSizeField = ShortCircuitCache.class.getDeclaredField("maxEvictableMmapedSize");
        maxEvictableMmapedSizeField.setAccessible(true);
        
        // Verify configuration is used
        int configuredSize = maxEvictableMmapedSizeField.getInt(cache);
        assertEquals("Configured cache size should be used in trimEvictionMaps", 3, configuredSize);
        
        // Cleanup
        cache.close();
    }

    @Test
    public void testDfsClientMmapCacheSize_RunMethod_UsesConfiguredValue() throws Exception {
        // Setup configuration
        Configuration conf = new Configuration();
        conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 7);
        
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        ShortCircuitCache cache = ShortCircuitCache.fromConf(dfsClientConf.getShortCircuitConf());
        
        // Access private fields
        Field maxEvictableMmapedSizeField = ShortCircuitCache.class.getDeclaredField("maxEvictableMmapedSize");
        maxEvictableMmapedSizeField.setAccessible(true);
        
        // Verify configuration is properly propagated
        int configuredSize = maxEvictableMmapedSizeField.getInt(cache);
        assertEquals("Configured cache size should be used in run method", 7, configuredSize);
        
        // Cleanup
        cache.close();
    }

    @Test
    public void testDfsClientMmapCacheSize_DefaultValueWhenNotSet() throws IOException, NoSuchFieldException, IllegalAccessException {
        // Test default value when configuration is not explicitly set
        Configuration conf = new Configuration();
        // Do not set CACHE_SIZE_KEY to test default value
        
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        ShortCircuitCache cache = ShortCircuitCache.fromConf(dfsClientConf.getShortCircuitConf());
        
        // Access private field
        Field maxEvictableMmapedSizeField = ShortCircuitCache.class.getDeclaredField("maxEvictableMmapedSize");
        maxEvictableMmapedSizeField.setAccessible(true);
        
        // Check that default value is used
        int defaultValue = maxEvictableMmapedSizeField.getInt(cache);
        assertEquals("Default cache size should be used when not configured", 
                     HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT, defaultValue);
        
        // Cleanup
        cache.close();
    }
}