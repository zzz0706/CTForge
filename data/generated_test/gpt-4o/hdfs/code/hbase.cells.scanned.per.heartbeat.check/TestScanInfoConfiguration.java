package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.ClassRule;

import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestScanInfoConfiguration {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = 
        HBaseClassTestRule.forClass(TestScanInfoConfiguration.class);

    @Test
    public void testGetCellsPerTimeoutCheck_DefaultValueFallback() {
        // 1. Prepare test conditions.
        // Create a Configuration with default values.
        Configuration conf = new Configuration();
        
        // Create a ColumnFamilyDescriptor for the ScanInfo constructor.
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = 
            ColumnFamilyDescriptorBuilder.newBuilder("testColumnFamily".getBytes());

        // Create a CellComparator for the ScanInfo constructor.
        CellComparator cellComparator = CellComparator.getInstance();

        // Initialize ScanInfo with proper configuration values using the available constructor.
        ScanInfo scanInfo = new ScanInfo(
            conf, 
            columnFamilyDescriptorBuilder.build(), 
            Long.MAX_VALUE, 
            Long.MAX_VALUE, 
            cellComparator
        );

        // 2. Invoke the `getCellsPerTimeoutCheck()` method.
        long cellsPerTimeoutCheck = scanInfo.getCellsPerTimeoutCheck();

        // 3. Verify that the timeout value correctly falls back to the default value.
        assertTrue("The method should return a valid fallback value greater than zero.",
            cellsPerTimeoutCheck > 0);
    }
}