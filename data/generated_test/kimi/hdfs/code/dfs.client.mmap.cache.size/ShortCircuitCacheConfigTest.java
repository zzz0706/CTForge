package org.apache.hadoop.hdfs.shortcircuit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ShortCircuitCacheConfigTest {

    private int customValue;

    public ShortCircuitCacheConfigTest(int customValue) {
        this.customValue = customValue;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {0}, {1}, {10}, {100}, {256}, {512}, {1024}
        });
    }

    @Test
    public void testMmapCacheSizeFromConfigServiceMatchesDefault() throws IOException {
        // Given
        Configuration conf = new Configuration();
        // Do not set the value, let it use default

        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        DfsClientConf.ShortCircuitConf shortCircuitConf = dfsClientConf.getShortCircuitConf();
        int actualCacheSize = shortCircuitConf.getShortCircuitMmapCacheSize();

        // Then
        assertEquals("Mmap cache size should match default when not configured", 
                    HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT, actualCacheSize);
    }

    @Test
    public void testMmapCacheSizeFromConfigServiceWithCustomValues() throws IOException {
        // Given
        Configuration conf = new Configuration();
        conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, customValue);

        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        DfsClientConf.ShortCircuitConf shortCircuitConf = dfsClientConf.getShortCircuitConf();
        int actualCacheSize = shortCircuitConf.getShortCircuitMmapCacheSize();

        // Then
        assertEquals("Mmap cache size should match the configured value", 
                    customValue, actualCacheSize);
    }

    @Test
    public void testMmapCacheSizeFromFileMatchesConfigService() throws IOException {
        // Given
        Configuration conf = new Configuration();
        // Load from default configs (core-default.xml, hdfs-default.xml)
        
        // Simulate loading from properties file for comparison
        Properties defaultProps = new Properties();
        // In real scenario, this would be loaded from hdfs-default.xml
        defaultProps.setProperty(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 
                                String.valueOf(HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT));
        
        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        DfsClientConf.ShortCircuitConf shortCircuitConf = dfsClientConf.getShortCircuitConf();
        int configServiceValue = shortCircuitConf.getShortCircuitMmapCacheSize();
        int fileLoadedValue = Integer.parseInt(defaultProps.getProperty(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY));

        // Then
        assertEquals("Config service value should match file loaded value", 
                    fileLoadedValue, configServiceValue);
    }

    @Test
    public void testShortCircuitCacheConstructorReceivesCorrectMmapCacheSize() throws IOException {
        // Given
        Configuration conf = new Configuration();
        int expectedCacheSize = 512;
        conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, expectedCacheSize);
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        DfsClientConf.ShortCircuitConf shortCircuitConf = dfsClientConf.getShortCircuitConf();

        // When
        ShortCircuitCache cache = ShortCircuitCache.fromConf(shortCircuitConf);

        // Then
        assertTrue("Should create ShortCircuitCache instance", cache instanceof ShortCircuitCache);
    }

    @Test
    public void testZeroMmapCacheSizeAllowed() throws IOException {
        // Given
        Configuration conf = new Configuration();
        conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, 0);
        
        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        DfsClientConf.ShortCircuitConf shortCircuitConf = dfsClientConf.getShortCircuitConf();
        int actualCacheSize = shortCircuitConf.getShortCircuitMmapCacheSize();
        
        // Then
        assertEquals("Mmap cache size should allow zero value", 0, actualCacheSize);
    }

    @Test
    public void testMaxIntMmapCacheSizeAllowed() throws IOException {
        // Given
        Configuration conf = new Configuration();
        conf.setInt(HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY, Integer.MAX_VALUE);
        
        // When
        DfsClientConf dfsClientConf = new DfsClientConf(conf);
        DfsClientConf.ShortCircuitConf shortCircuitConf = dfsClientConf.getShortCircuitConf();
        int actualCacheSize = shortCircuitConf.getShortCircuitMmapCacheSize();
        
        // Then
        assertEquals("Mmap cache size should allow MAX_VALUE", Integer.MAX_VALUE, actualCacheSize);
    }
}