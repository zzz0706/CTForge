package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@Category({MasterTests.class, SmallTests.class})
public class TestTimeToLiveLogCleaner {

    @ClassRule // HBaseClassTestRule ensures proper handling of test lifecycle
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestTimeToLiveLogCleaner.class);

    @Test
    public void testLogFileDeletionBoundaryCondition() {
        // 1. Prepare the test conditions
        Configuration configuration = new Configuration();
        configuration.setLong(TimeToLiveLogCleaner.TTL_CONF_KEY, 600000L); // Ensure TTL is set dynamically
       
        TimeToLiveLogCleaner logCleaner = new TimeToLiveLogCleaner();
        logCleaner.setConf(configuration); // Apply the configuration to the log cleaner

        long ttl = configuration.getLong(TimeToLiveLogCleaner.TTL_CONF_KEY, 600000L); // Get the TTL using the API

        // Mock a FileStatus object
        FileStatus fileStatus = mock(FileStatus.class);
        long currentTime = EnvironmentEdgeManager.currentTime();

        // Adjust the modification time to make the file age equal to TTL
        long modificationTime = currentTime - ttl + 1; // +1 ensures it's slightly older than TTL to pass criteria

        // 2. Set behavior for mocked FileStatus object
        when(fileStatus.getModificationTime()).thenReturn(modificationTime);
        when(fileStatus.getPath()).thenReturn(mock(org.apache.hadoop.fs.Path.class));
        when(fileStatus.getPath().getName()).thenReturn("testWAL.log");

        // 3. Test the behavior of isFileDeletable
        boolean isDeletable = logCleaner.isFileDeletable(fileStatus);

        // Validate the assertion
        assertTrue("File with modification time older than TTL should be deletable.", isDeletable);

        // Additional cleanup and finalization if necessary (generally managed automatically in HBase test framework)
    }
}