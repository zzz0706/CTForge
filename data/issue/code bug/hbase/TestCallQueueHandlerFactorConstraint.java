package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;
//HBASE-22559
@Category({MiscTests.class, SmallTests.class})
public class TestCallQueueHandlerFactorConstraint {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestCallQueueHandlerFactorConstraint.class);

    @Test
    public void testHandlerFactorWithinRange() {
        Configuration conf = HBaseConfiguration.create();

        // Read the configuration value (do not set it manually)
        float factor = conf.getFloat(RpcExecutor.CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, 2.0f);
        System.out.println("factor: " + factor);
        // Check the value is within [0.0, 1.0]
        boolean isValid = (factor >= 0.0f) && (factor <= 1.0f);

        assertTrue(
            "The value of " + RpcExecutor.CALL_QUEUE_HANDLER_FACTOR_CONF_KEY +
            " must be within [0.0, 1.0], but got: " + factor,
            isValid
        );
    }
}
