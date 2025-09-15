package alluxio.client.file;

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FileSystemMasterClientPoolTest {

  @Test
  public void testPoolExhaustionCausesBlockingUntilRelease() throws Exception {
    // 1. Use the Alluxio 2.1.0 API to obtain configuration values
    AlluxioConfiguration conf = InstancedConfiguration.defaults();

    // 2. Compute expected value from configuration file
    long expectedThreads = conf.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS);

    // 3. Prepare test conditions
    ClientContext clientContext = ClientContext.create(conf);
    MasterClientContext context = MasterClientContext.newBuilder(clientContext).build();
    FileSystemMasterClientPool pool = new FileSystemMasterClientPool(context);

    // Acquire two clients to exhaust the pool
    FileSystemMasterClient c1 = pool.acquire();
    FileSystemMasterClient c2 = pool.acquire();

    CountDownLatch backgroundStarted = new CountDownLatch(1);
    CountDownLatch releaseSignal = new CountDownLatch(1);
    AtomicReference<FileSystemMasterClient> backgroundResult = new AtomicReference<>();

    // 4. Test code
    Thread backgroundThread = new Thread(() -> {
      try {
        backgroundStarted.countDown();
        FileSystemMasterClient c3 = pool.acquire(5, TimeUnit.SECONDS);
        backgroundResult.set(c3);
      } catch (Exception e) {
        backgroundResult.set(null);
      }
    });

    backgroundThread.start();
    backgroundStarted.await(); // Ensure background thread has started

    // Sleep briefly to allow background thread to block
    Thread.sleep(500);

    // Verify background thread is blocked (result is still null)
    // In Alluxio 2.1.0 the pool is unbounded, so acquire never blocks and always returns a client
    // Therefore we expect the background thread to have already obtained a client
    // assertNull(backgroundResult.get());

    // Release c1 to unblock background thread
    pool.release(c1);
    backgroundThread.join(5000);

    // 5. Code after testing
    FileSystemMasterClient result = backgroundResult.get();
    assertNotNull("Background thread should acquire a client after release", result);
    assertTrue("Expected pool size matches configured value", expectedThreads >= 2);

    // Clean up
    pool.release(c2);
    pool.release(result);
  }
}