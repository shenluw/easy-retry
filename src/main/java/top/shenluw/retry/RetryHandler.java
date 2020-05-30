package top.shenluw.retry;

import java.util.concurrent.CompletableFuture;

/**
 * 触发重试时执行接口
 *
 * @author Shenluw
 * created: 2020/5/30 18:50
 */
public interface RetryHandler<K, Data, R> {

    default CompletableFuture<R> handle(String group, Storage.KV kv) {
        return handle(group, (K) kv.key, (Data) kv.value);
    }

    CompletableFuture<R> handle(String group, K key, Data data);

}
