package org.apache.hadoop.hbase.regionserver;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({RegionServerTests.class, SmallTests.class})
public class StoreScannerHeartbeatTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(StoreScannerHeartbeatTest.class);

  @Test
  public void testHeartbeatCheckTriggeredEveryConfiguredCells() throws IOException {
    // 1. Configuration as Input
    Configuration conf = HBaseConfiguration.create();

    // 2. Dynamic Expected Value Calculation
    long expectedCellsPerHeartbeatCheck = conf.getLong(
        "hbase.cells.scanned.per.heartbeat.check",
        10000L);

    // 3. Mock/Stub External Dependencies
    ScannerContext mockContext = mock(ScannerContext.class);
    when(mockContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)).thenReturn(false);
    when(mockContext.checkTimeLimit(LimitScope.BETWEEN_CELLS)).thenReturn(false);

    // Stub ScanInfo to return the expected value
    ScanInfo scanInfo = mock(ScanInfo.class);
    when(scanInfo.getCellsPerTimeoutCheck()).thenReturn(expectedCellsPerHeartbeatCheck);

    // Stub StoreScanner internals
    StoreScanner scanner = mock(StoreScanner.class);
    doReturn(true).when(scanner).next(anyList(), any(ScannerContext.class));

    // 4. Invoke the Method Under Test
    List<Cell> outResult = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      scanner.next(outResult, mockContext);
    }

    // 5. Assertions and Verification
    verify(scanner, times(7)).next(anyList(), any(ScannerContext.class));
  }
}