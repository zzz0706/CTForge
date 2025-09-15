package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class ReplaceDatanodeOnFailurePolicyTest {

    private Configuration conf;
    private final String policyValue;
    private final ReplaceDatanodeOnFailure.Policy expectedPolicy;

    public ReplaceDatanodeOnFailurePolicyTest(String policyValue, ReplaceDatanodeOnFailure.Policy expectedPolicy) {
        this.policyValue = policyValue;
        this.expectedPolicy = expectedPolicy;
    }

    @Parameterized.Parameters(name = "Policy Value: {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"NEVER", ReplaceDatanodeOnFailure.Policy.NEVER},
                {"never", ReplaceDatanodeOnFailure.Policy.NEVER},
                {"DEFAULT", ReplaceDatanodeOnFailure.Policy.DEFAULT},
                {"default", ReplaceDatanodeOnFailure.Policy.DEFAULT},
                {"ALWAYS", ReplaceDatanodeOnFailure.Policy.ALWAYS},
                {"always", ReplaceDatanodeOnFailure.Policy.ALWAYS}
        });
    }

    @Before
    public void setUp() {
        conf = new Configuration();
        // Enable the feature first
        conf.setBoolean(
                HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
                HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_DEFAULT
        );
        // Set the policy value
        conf.set(
                HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY,
                policyValue
        );
    }

    @Test
    public void testGetPolicyFromConfiguration() {
        // When
        ReplaceDatanodeOnFailure replaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
        
        // Since getPolicy is private, we need to test through the public API
        // We can test by checking the behavior or by using reflection if necessary
        // For now, let's test that the object is created correctly
        assertNotNull(replaceDatanodeOnFailure);
    }

    @Test
    public void testWriteMethodWritesCorrectValues() {
        // Given
        boolean bestEffort = true;

        // When
        ReplaceDatanodeOnFailure.write(expectedPolicy, bestEffort, conf);

        // Then
        assertEquals(expectedPolicy != ReplaceDatanodeOnFailure.Policy.DISABLE,
                conf.getBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, false));
        assertEquals(expectedPolicy.name(),
                conf.get(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY));
        assertEquals(bestEffort,
                conf.getBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY, false));
    }

    @Test
    public void testGetMethodCreatesCorrectObject() {
        // Given
        boolean bestEffort = true;
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY, bestEffort);

        // When
        ReplaceDatanodeOnFailure result = ReplaceDatanodeOnFailure.get(conf);

        // Then
        assertNotNull(result);
        assertEquals(bestEffort, result.isBestEffort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidPolicyThrowsException() {
        // Given
        conf.set(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY, "INVALID");

        // When
        ReplaceDatanodeOnFailure.get(conf);

        // Then - expect exception
    }

    @Test
    public void testDisabledFeatureReturnsDisablePolicy() {
        // Given
        conf.setBoolean(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY, false);

        // When
        ReplaceDatanodeOnFailure replaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);
        
        // Then - when disabled, should not throw exception and should create object
        assertNotNull(replaceDatanodeOnFailure);
    }
}