package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, SmallTests.class})
public class TestTimeToLiveLogCleaner {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = 
        HBaseClassTestRule.forClass(TestTimeToLiveLogCleaner.class);

    @Test
    public void testLogFileDeletionWithInvalidWALFilename() throws Exception {
        // Step 1: Create mock Configuration object and set up test environment.
        Configuration conf = new Configuration();
        // Retrieve the configuration value using API (not hardcoding the value).
        long ttl = conf.getLong(TimeToLiveLogCleaner.TTL_CONF_KEY, 600000);

        // Step 2: Create mock FileStatus with an invalid WAL filename.
        Path mockPath = new Path("/path/to/invalid-wal");
        FileStatus mockFileStatus = Mockito.mock(FileStatus.class);
        Mockito.when(mockFileStatus.getPath()).thenReturn(mockPath);

        // Step 3: Create an instance of TimeToLiveLogCleaner and set configuration.
        TimeToLiveLogCleaner logCleaner = new TimeToLiveLogCleaner();
        logCleaner.setConf(conf);

        // Step 4: Call isFileDeletable with mock FileStatus object.
        boolean isDeletable = logCleaner.isFileDeletable(mockFileStatus);

        // Step 5: Validate the expected result.
        assertTrue("The log file with an invalid WAL filename should be deletable.", isDeletable);
    }
}