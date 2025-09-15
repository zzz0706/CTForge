package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class MasterJournalCheckpointConfigTest {

    @Test
    public void testCheckpointPeriodEntriesConfig() {
        // Prepare an instance of the configuration using the default Alluxio configuration.
        AlluxioConfiguration conf = InstancedConfiguration.defaults();

        // Declare the configuration key based on the PropertyKey class.
        final PropertyKey checkpointPeriodEntriesKey = PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES;

        // Use the AlluxioConfiguration API to fetch the configuration value.
        long checkpointPeriodEntries = conf.getLong(checkpointPeriodEntriesKey);

        // Validate the value of the configuration. It must be greater than 0.
        boolean isValid = checkpointPeriodEntries > 0;

        // Perform assertions to ensure the configuration value is valid.
        Assert.assertTrue(
            String.format(
                "Invalid configuration value for '%s': %d. It must be a positive value.",
                checkpointPeriodEntriesKey.getName(),
                checkpointPeriodEntries
            ),
            isValid
        );
    }
}