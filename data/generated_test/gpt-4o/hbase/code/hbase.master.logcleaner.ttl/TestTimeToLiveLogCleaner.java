package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category({MasterTests.class, SmallTests.class})
public class TestTimeToLiveLogCleaner {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestTimeToLiveLogCleaner.class);

    @Test
    public void testLogFileDeletionWithTTLExceeded() throws Exception {
        // 1. You need to use the HBase 2.2.2 API correctly to obtain configuration values.
        Configuration conf = new Configuration();
        
        // Assuming the TTL configuration key and default TTL are correctly set in EnvironmentConfigurable
        conf.setLong(TimeToLiveLogCleaner.TTL_CONF_KEY, 60000L); // Example TTL: 60 seconds
        TimeToLiveLogCleaner ttlLogCleaner = new TimeToLiveLogCleaner();
        ttlLogCleaner.setConf(conf);

        long ttlValue = conf.getLong(
            TimeToLiveLogCleaner.TTL_CONF_KEY, 
            TimeToLiveLogCleaner.DEFAULT_TTL
        );

        // 2. Prepare the test conditions.
        ManualEnvironmentEdge manualEdge = new ManualEnvironmentEdge();
        EnvironmentEdgeManager.injectEdge(manualEdge);

        long mockCurrentTime = 1000000L;
        manualEdge.setValue(mockCurrentTime);

        // Mock FileStatus object for a log file with an old modification time
        FileStatus mockFileStatus = mock(FileStatus.class);
        long mockFileModificationTime = mockCurrentTime - (ttlValue + 1000); // Older than TTL
        when(mockFileStatus.getModificationTime()).thenReturn(mockFileModificationTime);
        when(mockFileStatus.getPath()).thenReturn(new Path("/mock/path/oldLogFile"));

        // 3. Test the isFileDeletable method.
        boolean isDeletable = ttlLogCleaner.isFileDeletable(mockFileStatus);

        // 4. Verify the result.
        assertTrue(
            "The log file should be deletable if its age exceeds the TTL.",
            isDeletable
        );

        // Code after testing, cleanup.
        EnvironmentEdgeManager.reset(); // Cleanup the injected manual edge.
    }
}