package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
public class TopConfNNTopWindowsMinutesTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testNNTopWindowsMinutesDefaultValue() {
        // Ensure no explicit value is set, so default is used
        conf.unset(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY);

        // Create TopConf instance which will process the configuration
        TopConf topConf = new TopConf(conf);

        // Get expected default value from DFSConfigKeys
        String[] defaultValues = DFSConfigKeys.NNTOP_WINDOWS_MINUTES_DEFAULT;
        String expectedDefault = String.join(",", defaultValues);
        String[] expectedPeriods = defaultValues;

        // Verify that the configuration service returns the same default
        String configValue = conf.get(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, expectedDefault);
        assertEquals("ConfigService should return the default value", expectedDefault, configValue);

        // Verify the internal array has correct number of elements
        assertEquals("Should have correct number of reporting periods", expectedPeriods.length, topConf.nntopReportingPeriodsMs.length);

        // Verify each period is correctly converted to milliseconds
        for (int i = 0; i < expectedPeriods.length; i++) {
            int expectedMinutes = Integer.parseInt(expectedPeriods[i].trim());
            long expectedMillis = TimeUnit.MINUTES.toMillis(expectedMinutes);
            assertEquals("Period " + i + " should be converted to milliseconds correctly", expectedMillis, topConf.nntopReportingPeriodsMs[i]);
        }
    }

    @Test
    public void testNNTopWindowsMinutesCustomValue() {
        // Set custom configuration value
        String customValue = "2,10,60";
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, customValue);

        // Create TopConf instance
        TopConf topConf = new TopConf(conf);

        // Verify that the configuration service returns the custom value
        String configValue = conf.get(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY);
        assertEquals("ConfigService should return the custom value", customValue, configValue);

        // Verify the internal array has correct number of elements
        String[] expectedPeriods = customValue.split(",");
        assertEquals("Should have correct number of reporting periods", expectedPeriods.length, topConf.nntopReportingPeriodsMs.length);

        // Verify each period is correctly converted to milliseconds
        for (int i = 0; i < expectedPeriods.length; i++) {
            int expectedMinutes = Integer.parseInt(expectedPeriods[i].trim());
            long expectedMillis = TimeUnit.MINUTES.toMillis(expectedMinutes);
            assertEquals("Period " + i + " should be converted to milliseconds correctly", expectedMillis, topConf.nntopReportingPeriodsMs[i]);
        }
    }

    @Test
    public void testNNTopWindowsMinutesSingleValue() {
        // Set single value configuration
        String singleValue = "5";
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, singleValue);

        // Create TopConf instance
        TopConf topConf = new TopConf(conf);

        // Verify the internal array has one element
        assertEquals("Should have one reporting period", 1, topConf.nntopReportingPeriodsMs.length);

        // Verify the period is correctly converted to milliseconds
        int expectedMinutes = 5;
        long expectedMillis = TimeUnit.MINUTES.toMillis(expectedMinutes);
        assertEquals("Single period should be converted to milliseconds correctly", expectedMillis, topConf.nntopReportingPeriodsMs[0]);
    }

    @Test(expected = NumberFormatException.class)
    public void testNNTopWindowsMinutesInvalidNonNumericValue() {
        // Set invalid non-numeric value
        String invalidValue = "1,abc,5";
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, invalidValue);

        // Creating TopConf should throw NumberFormatException
        new TopConf(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNNTopWindowsMinutesZeroValue() {
        // Set invalid zero value
        String zeroValue = "0";
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, zeroValue);

        // Creating TopConf should throw IllegalArgumentException due to validation
        new TopConf(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNNTopWindowsMinutesNegativeValue() {
        // Set invalid negative value
        String negativeValue = "-5";
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, negativeValue);

        // Creating TopConf should throw IllegalArgumentException due to validation
        new TopConf(conf);
    }
}