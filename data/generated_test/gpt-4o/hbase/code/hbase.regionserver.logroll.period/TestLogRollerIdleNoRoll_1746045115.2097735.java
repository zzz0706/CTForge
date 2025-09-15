package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Category({RegionServerTests.class, SmallTests.class})
public class TestLogRollerIdleNoRoll {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestLogRollerIdleNoRoll.class);

    private Server mockServer;
    private RegionServerServices mockServices;
    private LogRoller logRoller;
    private WAL mockWal;
    private Configuration mockConfiguration;

    @Before
    public void setUp() throws Exception {
        // 1. Create mock Server, RegionServerServices, WAL, and Configuration
        mockServer = mock(Server.class);
        mockServices = mock(RegionServerServices.class);
        mockWal = mock(WAL.class);
        mockConfiguration = mock(Configuration.class);

        // 2. Mock configuration retrieval APIs using the HBase 2.2.2 API correctly
        when(mockServer.getConfiguration()).thenReturn(mockConfiguration);
        when(mockConfiguration.getLong("hbase.regionserver.logroll.period", 3600000))
                .thenReturn(60000L); // Using configuration API
        when(mockConfiguration.getInt("hbase.regionserver.thread.wake.frequency", 10000))
                .thenReturn(10000); // Using configuration API
        when(mockConfiguration.getLong("hbase.regionserver.hlog.check.lowreplication.interval", 30000L))
                .thenReturn(30000L); // Using configuration API

        // 3. Initialize LogRoller with mocked Server and RegionServerServices
        logRoller = new LogRoller(mockServer, mockServices);

        // 4. Prepare the testing state
        // Set lastRollTime to the current time to emulate a scenario where no time has elapsed
        Field lastRollTimeField = LogRoller.class.getDeclaredField("lastRollTime");
        lastRollTimeField.setAccessible(true);
        lastRollTimeField.set(logRoller, System.currentTimeMillis());

        // Prepare a map indicating that no WAL roll is needed
        Field walNeedsRollField = LogRoller.class.getDeclaredField("walNeedsRoll");
        walNeedsRollField.setAccessible(true);
        ConcurrentMap<WAL, Boolean> walNeedsRollMap = new ConcurrentHashMap<>();
        walNeedsRollMap.put(mockWal, Boolean.FALSE); // Indicate no roll requests
        walNeedsRollField.set(logRoller, walNeedsRollMap);
    }

    @Test
    public void testLogRollerIdleNoRoll() throws Exception {
        // 1. Set up the run condition to ensure the LogRoller executes a single iteration
        Field runningField = LogRoller.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(logRoller, false); // Terminate loop after one iteration

        // 2. Run the LogRoller thread
        Thread logRollerThread = new Thread(() -> {
            try {
                logRoller.run(); // Execute the run method being tested
            } catch (Exception e) {
                e.printStackTrace(); // Handle exceptions in the test thread
            }
        });

        logRollerThread.start();
        logRollerThread.join(); // Wait for the thread to complete

        // 3. Verify that the WAL.rollWriter() method is never called
        verify(mockWal, never()).rollWriter(anyBoolean());
    }

    @After
    public void tearDown() throws Exception {
        // 4. Tear down test resources to prevent memory leakage
        mockServer = null;
        mockServices = null;
        logRoller = null;
        mockWal = null;
        mockConfiguration = null;
    }
}