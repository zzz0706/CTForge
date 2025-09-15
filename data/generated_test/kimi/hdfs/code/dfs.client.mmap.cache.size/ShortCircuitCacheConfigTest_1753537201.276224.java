package org.apache.hadoop.hdfs.shortcircuit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class ShortCircuitCacheConfigTest {

    @Test
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
}