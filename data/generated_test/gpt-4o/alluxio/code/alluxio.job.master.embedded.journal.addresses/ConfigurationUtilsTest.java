package alluxio.util;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

public class ConfigurationUtilsTest {
    @Mock
    private AlluxioConfiguration mConfiguration;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this); // Corrected the method to initMocks()
    }

    @Test
    public void testGetJobMasterEmbeddedJournalAddressesConfigured() {
        // Prepare the test conditions.
        // Mock the configuration to behave as if 'alluxio.job.master.embedded.journal.addresses' is explicitly set.
        when(mConfiguration.isSet(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES)).thenReturn(true);
        when(mConfiguration.getList(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES, ","))
            .thenReturn(Arrays.asList("host1:19998", "host2:19998", "host3:19998")); // Changed to Arrays.asList()

        // Test code.
        List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterEmbeddedJournalAddresses(mConfiguration);

        // Validate the results.
        Assert.assertNotNull("The returned list should not be null.", addresses);
        Assert.assertEquals("The size of the returned list should match.", 3, addresses.size());
        Assert.assertEquals("The first address should be correctly parsed.", 
            new InetSocketAddress("host1", 19998), addresses.get(0));
        Assert.assertEquals("The second address should be correctly parsed.", 
            new InetSocketAddress("host2", 19998), addresses.get(1));
        Assert.assertEquals("The third address should be correctly parsed.", 
            new InetSocketAddress("host3", 19998), addresses.get(2));

        // Code after testing.
        verify(mConfiguration, times(1)).isSet(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES);
        verify(mConfiguration, times(1)).getList(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES, ",");
    }
}