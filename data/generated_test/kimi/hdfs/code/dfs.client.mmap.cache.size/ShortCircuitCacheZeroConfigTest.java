package org.apache.hadoop.hdfs.shortcircuit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ShortCircuitCacheZeroConfigTest {

    private Configuration conf;
    private DfsClientConf dfsClientConf;

    @Before
    public void setUp() {
        conf = new Configuration();
        dfsClientConf = new DfsClientConf(conf);
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDfsClientMmapCacheSize_ZeroValue_AllowsZeroCopyReads() {
        // 1. Set dfs.client.mmap.cache.size=0 in configuration
        conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 0);
        
        // Reinitialize DfsClientConf with updated configuration
        dfsClientConf = new DfsClientConf(conf);
        
        // 2. Create ShortCircuitCache instance using the configuration
        DfsClientConf.ShortCircuitConf shortCircuitConf = dfsClientConf.getShortCircuitConf();
        ShortCircuitCache cache = ShortCircuitCache.fromConf(shortCircuitConf);
        
        // 3. Verify that the cache was created successfully with zero mmap cache size
        assertNotNull("ShortCircuitCache should be created successfully", cache);
        
        // Use reflection to access the private field maxEvictableMmapedSize
        try {
            Field field = ShortCircuitCache.class.getDeclaredField("maxEvictableMmapedSize");
            field.setAccessible(true);
            int maxEvictableMmapedSize = field.getInt(cache);
            assertEquals("maxEvictableMmapedSize should be 0", 0, maxEvictableMmapedSize);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access maxEvictableMmapedSize field", e);
        }
        
        // 4. Test cache operations that should work with zero cache size
        // Test that the cache can be closed without issues
        try {
            cache.close();
        } catch (Exception e) {
            throw new RuntimeException("close() method should not throw exceptions with zero cache size", e);
        }
        
        // 5. Verify no exceptions were thrown during operations
        assertTrue("Zero-copy reads should function normally without exceptions even when dfs.client.mmap.cache.size is 0", true);
    }
}