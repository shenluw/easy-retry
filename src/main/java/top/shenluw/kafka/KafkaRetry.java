package top.shenluw.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SuccessCallback;
import top.shenluw.kafka.storage.MemoryStorage;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Shenluw
 * created: 2020/4/25 17:40
 */
public class KafkaRetry<K extends Serializable, V extends Serializable> {

    private static final Logger log = LoggerFactory.getLogger(KafkaRetry.class);

    private Storage             storage;
    private KafkaTemplate<K, V> kafkaTemplate;

    private ScheduledExecutorService scheduledExecutorService;

    /**
     * 一次重试的最大记录数量
     */
    private int  maxRetryCount      = 100;
    /**
     * 定时重试间隔, 单位毫秒
     */
    private int  retryInterval      = 60_000;
    /**
     * 同一记录重试间隔，单位毫秒
     */
    private long thresholdTimestamp = 10_000;

    private SuccessCallback<SendResult<K, V>> successCallback;
    private FailureCallback                   failureCallback;

    private volatile boolean start;

    public KafkaRetry(KafkaTemplate<K, V> kafkaTemplate) {
        this(new MemoryStorage(new ConcurrentHashMap<>()), kafkaTemplate, Executors.newSingleThreadScheduledExecutor());
    }

    public KafkaRetry(Storage storage, KafkaTemplate<K, V> kafkaTemplate) {
        this(storage, kafkaTemplate, Executors.newSingleThreadScheduledExecutor());
    }

    public KafkaRetry(Storage storage, KafkaTemplate<K, V> kafkaTemplate,
                      ScheduledExecutorService scheduledExecutorService) {
        this.storage = storage;
        this.kafkaTemplate = kafkaTemplate;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void push(String topic, V data) {
        storage.save(topic, null, data);
    }

    public void push(String topic, K key, V data) {
        storage.save(topic, key, data);
    }

    public synchronized void start() {
        if (start) {
            return;
        }
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (log.isInfoEnabled()) {
                log.info("retry total count: {}", storage.count());
            }

            Set<String> topics = storage.topics();
            for (String topic : topics) {
                retry(topic);
            }
        }, retryInterval, retryInterval, TimeUnit.MILLISECONDS);
    }

    public synchronized void shutdown() {
        if (start) {
            scheduledExecutorService.shutdown();
            start = false;
        }
    }

    private void retry(String topic) {
        long current = System.currentTimeMillis();
        for (int i = 0; i < maxRetryCount; i++) {
            Storage.KV kv;
            if (thresholdTimestamp > 0) {
                kv = storage.peek(topic);
                if (kv == null) {
                    return;
                }
                // 避免短时间反复重试
                if (kv.timestamp + thresholdTimestamp > current) {
                    continue;
                }
                storage.delete(topic, kv);
            } else {
                kv = storage.pop(topic);
                if (kv == null) {
                    return;
                }
            }

            K key  = (K) kv.key;
            V data = (V) kv.value;

            kafkaTemplate.send(topic, key, data).addCallback(new ListenableFutureCallback<SendResult<K, V>>() {

                @Override
                public void onFailure(Throwable ex) {
                    log.debug("retry failure. topic: {}, key: {}, v: {}", topic, key, data, ex);
                    kv.timestamp = System.currentTimeMillis();
                    storage.save(topic, kv);
                    if (failureCallback != null) {
                        failureCallback.onFailure(ex);
                    }
                }

                @Override
                public void onSuccess(SendResult<K, V> result) {
                    log.debug("retry success. topic: {}, key: {}, v: {}", topic, key, data);
                    if (successCallback != null) {
                        successCallback.onSuccess(result);
                    }
                }
            });
        }

    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public KafkaRetry<K, V> setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
        return this;
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    public KafkaRetry<K, V> setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
        return this;
    }

    public SuccessCallback<SendResult<K, V>> getSuccessCallback() {
        return successCallback;
    }

    public KafkaRetry<K, V> setSuccessCallback(SuccessCallback<SendResult<K, V>> successCallback) {
        this.successCallback = successCallback;
        return this;
    }

    public FailureCallback getFailureCallback() {
        return failureCallback;
    }

    public void setFailureCallback(FailureCallback failureCallback) {
        this.failureCallback = failureCallback;
    }

    public long getThresholdTimestamp() {
        return thresholdTimestamp;
    }

    public void setThresholdTimestamp(long thresholdTimestamp) {
        this.thresholdTimestamp = thresholdTimestamp;
    }

    public Storage getStorage() {
        return storage;
    }
}