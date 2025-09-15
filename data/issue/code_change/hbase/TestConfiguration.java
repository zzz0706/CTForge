package org.apache.hadoop.hbase.rest.client;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
//hbase-20590 
@Category({ClientTests.class, SmallTests.class})
public class TestConfiguration {

    @ClassRule
    public static final HBaseCommonTestingUtility TEST_UTIL = new HBaseCommonTestingUtility();

    private Configuration conf;
    private Client mockClient;

    @Before
    public void setUp() {
       
        conf = TEST_UTIL.getConfiguration();
        mockClient = mock(Client.class);
    }

    @Test
    public void testEmptyPrincipalStringShouldBeRejected() {
        conf.set("hbase.rest.security.enabled", "true");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set(Constants.REST_KERBEROS_PRINCIPAL, "");

        try {
            new RemoteAdmin(mockClient, conf);
            fail("Test failed: expected IllegalArgumentException for empty REST Kerberos principal.");
        } catch (IllegalArgumentException e) {
            System.out.println("Test passed: empty principal was rejected.");
        } catch (Exception e) {
            fail("Unexpected exception: " + e);
        }
    }

}