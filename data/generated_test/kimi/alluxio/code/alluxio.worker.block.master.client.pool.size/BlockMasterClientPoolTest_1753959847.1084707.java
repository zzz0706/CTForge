package alluxio.worker.block;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockMasterClient.class, BlockMasterClientPool.class})
public class BlockMasterClientPoolTest {

  @Test
  public void testAcquireBlocksWhenPoolExhausted() throws Exception {
    // 1. Use Alluxio 2.1.0 API to read configuration
    AlluxioConfiguration conf = InstancedConfiguration.defaults();
    long expectedMaxClients = conf.getInt(PropertyKey.WORKER_BLOCK_MASTER_CLIENT_POOL_SIZE);

    // 2. Prepare test conditions: mock BlockMasterClient creation
    BlockMasterClient mockClient = mock(BlockMasterClient.class);
    PowerMockito.whenNew(BlockMasterClient.class)
            .withAnyArguments()
            .thenReturn(mockClient);

    // 3. Test code
    BlockMasterClientPool pool = new BlockMasterClientPool();

    // Acquire expectedMaxClients to fill the pool
    BlockMasterClient client1 = pool.acquire();
    BlockMasterClient client2 = pool.acquire();
    BlockMasterClient client3 = pool.acquire();

    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<BlockMasterClient> acquiredClient = new AtomicReference<>();

    Thread backgroundThread = new Thread(() -> {
      try {
        // Attempt to acquire 4th client with 1-second timeout
        BlockMasterClient client4 = pool.acquire(1, TimeUnit.SECONDS);
        acquiredClient.set(client4);
        latch.countDown();
      } catch (Exception e) {
        latch.countDown();
      }
    });

    backgroundThread.start();

    // Ensure background thread is blocked
    Thread.sleep(100);

    // Return one client
    pool.release(client1);

    // Wait for background thread to finish
    backgroundThread.join(2000);

    // 4. Verify results
    BlockMasterClient actualClient = acquiredClient.get();
    assertNotNull("4th acquisition should succeed after returning a client", actualClient);
  }
}