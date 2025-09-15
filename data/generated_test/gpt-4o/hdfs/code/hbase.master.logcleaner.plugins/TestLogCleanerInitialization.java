package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.cleaner.DirScanPool;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

@Category({MasterTests.class, MediumTests.class})
public class TestLogCleanerInitialization {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestLogCleanerInitialization.class);

    @Test
    public void test_LogCleaner_initialization_withValidConfiguration() throws Exception {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = HBaseConfiguration.create();
        Path oldLogDir = new Path("/hbase/archived-logs");
        FileSystem fs = FileSystem.get(conf);

        // 2. Prepare the test conditions.
        Stoppable stoppable = new Stoppable() {
            private volatile boolean stopped = false;

            @Override
            public void stop(String why) {
                stopped = true;
            }

            @Override
            public boolean isStopped() {
                return stopped;
            }
        };
        DirScanPool pool = new DirScanPool(conf);

        // 3. Test code: Initialize LogCleaner and perform assertions.
        LogCleaner logCleaner = new LogCleaner(1000, stoppable, conf, fs, oldLogDir, pool);

        // Assert that LogCleaner instance initializes properly.
        assertNotNull("LogCleaner should not be null", logCleaner);

        // Assert for expected behavior.
        // Commenting out getDelegates assertion as getDelegates() is not part of LogCleaner in HBase 2.2.2
        // assertNotNull("Should have CleanerDelegates inside LogCleaner", logCleaner.getDelegates()); 
        
        // Validate the period.
        assertEquals("Cleaner interval should match the provided interval", 1000, logCleaner.getPeriod());

        // 4. Code after testing.
    }
}