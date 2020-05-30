package top.shenluw.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.shenluw.retry.storage.MemoryStorage;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * @author Shenluw
 * created: 2020/4/25 17:40
 */
public class Retry<K extends Serializable, V extends Serializable, R> {

    private static final Logger log = LoggerFactory.getLogger(Retry.class);

    private Storage storage;

    private RetryHandler<K, V, R> retryHandler;

    private ScheduledExecutorService scheduledExecutorService;

    /**
     * 一次重试的最大记录数量
     */
    private int  maxRetryCount      = 100;
    /**
     * 一条记录的最大重试次数
     * -1 无限 0 不重试
     */
    private int  maxRetryTimes      = -1;
    /**
     * 定时重试间隔, 单位毫秒
     */
    private int  retryInterval      = 60_000;
    /**
     * 同一记录重试间隔，单位毫秒
     */
    private long thresholdTimestamp = 10_000;

    /**
     * 重试结果回调
     */
    private BiConsumer<R, Throwable> callback;

    private volatile boolean start;

    /**
     * 缓存每个组的第一个数据时间戳，单位毫秒
     */
    private final Map<String, Long> cacheFirstTs = new ConcurrentHashMap<>();

    public Retry(RetryHandler<K, V, R> retryHandler) {
        this(new MemoryStorage(new ConcurrentHashMap<>()), retryHandler, Executors.newSingleThreadScheduledExecutor());
    }

    public Retry(Storage storage, RetryHandler<K, V, R> retryHandler) {
        this(storage, retryHandler, Executors.newSingleThreadScheduledExecutor());
    }

    public Retry(Storage storage, RetryHandler<K, V, R> retryHandler,
                 ScheduledExecutorService scheduledExecutorService) {
        this.storage = storage;
        this.retryHandler = retryHandler;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void push(String group, V data) {
        storage.save(group, null, data);
    }

    public void push(String group, K key, V data) {
        storage.save(group, key, data);
    }

    public synchronized void start() {
        if (start) {
            return;
        }
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (log.isInfoEnabled()) {
                log.info("retry total count: {}", storage.count());
            }

            if (maxRetryTimes == 0) {
                return;
            }

            Set<String> groups = storage.groups();
            for (String group : groups) {
                retry(group);
            }
        }, retryInterval, retryInterval, TimeUnit.MILLISECONDS);
    }

    public synchronized void shutdown() {
        if (start) {
            scheduledExecutorService.shutdown();
            start = false;
        }
    }

    private void retry(String group) {
        long current;
        for (int i = 0; i < maxRetryCount; i++) {
            Storage.KV kv;
            if (thresholdTimestamp > 0) {
                current = System.currentTimeMillis();
                Long cacheTs = cacheFirstTs.get(group);
                if (cacheTs != null && cacheTs + thresholdTimestamp > current) {
                    return;
                }

                kv = storage.peek(group);
                //  顺序存入， 当有一个时间不满足后面的全不满足
                if (kv == null) {
                    return;
                }
                // 避免短时间反复重试
                if (kv.putTimestamp + thresholdTimestamp > current) {
                    cacheFirstTs.put(group, kv.putTimestamp);
                    return;
                }
                storage.delete(group, kv);
            } else {
                kv = storage.pop(group);
                if (kv == null) {
                    return;
                }
            }

            retryHandler.handle(group, kv)
                    .whenComplete((r, throwable) -> {
                        Serializable key  = kv.key;
                        Serializable data = kv.value;
                        if (throwable != null) {
                            log.debug("retry failure. group: {}, key: {}, v: {}, times: {}", group, key, data, kv.retryTimes, throwable);
                            if (maxRetryTimes > 0 && ++kv.retryTimes >= maxRetryTimes) {
                                log.info("retry times more than max {}. group: {}, key: {}, v: {}, ts: {}",
                                        maxRetryTimes, group, key, data, kv.timestamp);
                                throwable = new Throwable("retry times more than max", throwable);
                            } else {
                                kv.putTimestamp = System.currentTimeMillis();
                                storage.save(group, kv);
                            }
                        } else {
                            log.debug("retry success. group: {}, key: {}, v: {}", group, key, data);
                        }
                        if (callback != null) {
                            callback.accept(r, throwable);
                        }
                    });
        }

    }

    public Storage getStorage() {
        return storage;
    }

    public RetryHandler<K, V, R> getRetryHandler() {
        return retryHandler;
    }

    public void setRetryHandler(RetryHandler<K, V, R> retryHandler) {
        this.retryHandler = retryHandler;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    public long getThresholdTimestamp() {
        return thresholdTimestamp;
    }

    public void setThresholdTimestamp(long thresholdTimestamp) {
        this.thresholdTimestamp = thresholdTimestamp;
    }

    public BiConsumer<R, Throwable> getCallback() {
        return callback;
    }

    public void setCallback(BiConsumer<R, Throwable> callback) {
        this.callback = callback;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }
}