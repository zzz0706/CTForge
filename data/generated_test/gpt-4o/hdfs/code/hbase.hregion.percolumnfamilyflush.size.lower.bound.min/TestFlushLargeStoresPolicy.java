package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.FlushPolicy;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;

@Category({RegionServerTests.class, SmallTests.class})
public class TestFlushLargeStoresPolicy {

    @ClassRule // HBaseClassTestRule ensures proper test setup and teardown.
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestFlushLargeStoresPolicy.class);

    @Test
    public void testSetFlushSizeLowerBoundsWithDefaultConfiguration() {
        // Step 1: Prepare the test conditions.
        TableName tableName = TableName.valueOf("testTable");

        // Create a table descriptor with multiple column families.
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf1")).build());
        tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf2")).build());
        tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf3")).build());
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        // Create a mock HRegion.
        HRegion mockRegion = mock(HRegion.class);
        when(mockRegion.getTableDescriptor()).thenReturn(tableDescriptor);
        when(mockRegion.getMemStoreFlushSize()).thenReturn(32 * 1024 * 1024L); // Mock MemStore flush size (32MB).

        // Prepare configuration.
        Configuration configuration = new Configuration();
        configuration.setLong(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
                              FlushLargeStoresPolicy.DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN);

        // Initialize an instance of FlushLargeStoresPolicy.
        TestableFlushLargeStoresPolicy flushLargeStoresPolicy = new TestableFlushLargeStoresPolicy();
        flushLargeStoresPolicy.setConf(configuration);

        // Step 2: Execute the behavior to test.
        flushLargeStoresPolicy.setFlushSizeLowerBounds(mockRegion);

        // Step 3: Perform assertions to validate results.
        // Calculate expected flush size lower-bound.
        long expectedAverageFlushSize = mockRegion.getMemStoreFlushSize() / tableDescriptor.getColumnFamilies().length;
        long defaultConfigValue = configuration.getLong(
                FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
                FlushLargeStoresPolicy.DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN
        );

        // Retrieve actual flush size lower-bound and validate.
        long actualFlushSizeLowerBound = flushLargeStoresPolicy.getFlushSizeLowerBound();
        Assert.assertEquals(Math.max(expectedAverageFlushSize, defaultConfigValue), actualFlushSizeLowerBound);
    }

    // Concrete subclass for testing purposes, as FlushLargeStoresPolicy is abstract.
    static class TestableFlushLargeStoresPolicy extends FlushLargeStoresPolicy {

        private long flushSizeLowerBound;

        @Override
        public void setFlushSizeLowerBounds(HRegion region) {
            TableDescriptor tableDescriptor = region.getTableDescriptor();
            long averageFlushSize = region.getMemStoreFlushSize() / tableDescriptor.getColumnFamilies().length;

            // Use configuration values to determine the flush size lower-bound.
            long configuredLowerBoundMin = getConf().getLong(
                    HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
                    DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN
            );
            flushSizeLowerBound = Math.max(averageFlushSize, configuredLowerBoundMin);
        }

        public long getFlushSizeLowerBound() {
            return flushSizeLowerBound;
        }

        @Override
        public Collection<HStore> selectStoresToFlush() {
            // Implementation for this abstract method must exist; return null for testing purposes.
            return null;
        }
    }
}