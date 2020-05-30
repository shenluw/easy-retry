package top.shenluw.kafkaretry;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import top.shenluw.retry.Retry;
import top.shenluw.retry.RetryHandler;
import top.shenluw.retry.storage.MemoryStorage;
import top.shenluw.retry.storage.RocksDBStorage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Shenluw
 * created: 2020/5/30 21:52
 */
public class RetryTests {
    private static final Logger log = getLogger(RetryTests.class);

    BiConsumer<String, Throwable>        callback = (s, throwable) -> {
        System.out.println("retry result: " + s);
        if (throwable != null) {
            throwable.printStackTrace();
        }
    };
    RetryHandler<String, String, String> handler  = (group, key, s) -> {
        CompletableFuture<String> future = new CompletableFuture<>();
        log.info("retry run");
        new Thread(() -> {
            log.info("do retry");
            try {
                Thread.sleep(450);
            } catch (InterruptedException e) {
            }
            log.info("retry ok");
//                        future.complete("retry");
            future.completeExceptionally(new Throwable("err"));
        }).start();
        try {
            Thread.sleep(120);
        } catch (InterruptedException e) {
        }
        return future;
    };

    @Test
    void memory() throws InterruptedException {

        Retry<String, String, String> retry = new Retry<>(new MemoryStorage(new ConcurrentHashMap<>()),
                handler, Executors.newScheduledThreadPool(10));

        retry.setCallback(callback);
        retry.setRetryInterval(50);
        retry.setThresholdTimestamp(3_000);
        retry.start();

        retry.push("test", "test key", "test data");

        Thread.sleep(5000);

    }

    @Test
    void db() throws Exception {
        RocksDBStorage storage = new RocksDBStorage("build/test");
        storage.open();
        log.info("db count {}", storage.count("test"));
        Retry<String, String, String> retry = new Retry<>(storage,
                handler, Executors.newScheduledThreadPool(10));

        retry.setCallback(callback);
        retry.setRetryInterval(50);
        retry.setThresholdTimestamp(3_000);
        retry.start();

        retry.push("test", "test key", "test data");

        Thread.sleep(5000);

        storage.close();
        retry.shutdown();
    }
}
