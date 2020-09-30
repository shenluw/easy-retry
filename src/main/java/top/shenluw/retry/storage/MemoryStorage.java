package top.shenluw.retry.storage;

import org.springframework.util.CollectionUtils;
import top.shenluw.retry.Storage;

import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * @author Shenluw
 * created: 2020/4/25 17:54
 */
public class MemoryStorage implements Storage {

    public interface CreateCacheStrategy {
        /**
         * 分配队列
         *
         * @return 数据缓存队列
         */
        Queue<KV> createQueue();
    }

    private final Map<String, Queue<Storage.KV>> cache;
    private final CreateCacheStrategy            strategy;

    public MemoryStorage(Map<String, Queue<KV>> cache, CreateCacheStrategy strategy) {
        this.cache = cache;
        this.strategy = strategy;
    }

    @Override
    public void open() throws Exception {
        // ignore
    }

    @Override
    public void close() throws Exception {
        cache.clear();
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void save(String group, KV kv) {

        Queue<KV> queue = cache.get(group);
        if (queue == null) {
            synchronized (cache) {
                queue = cache.get(group);
                if (queue == null) {
                    queue = strategy.createQueue();
                    cache.put(group, queue);
                }
            }
        }
        queue.add(kv);
    }

    @Override
    public KV pop(String group) {
        Queue<KV> queue = cache.get(group);
        if (!CollectionUtils.isEmpty(queue)) {
            return queue.poll();
        }
        return null;
    }

    @Override
    public KV peek(String group) {
        Queue<KV> queue = cache.get(group);
        if (!CollectionUtils.isEmpty(queue)) {
            return queue.peek();
        }
        return null;
    }

    @Override
    public void delete(String group, KV kv) {
        Queue<KV> queue = cache.get(group);
        if (!CollectionUtils.isEmpty(queue)) {
            queue.removeIf(next -> next == kv);
        }
    }

    @Override
    public Set<String> groups() {
        return cache.keySet();
    }

    @Override
    public long count() {
        long count = 0;
        if (cache.isEmpty()) {
            return count;
        }
        for (Queue<KV> kvs : cache.values()) {
            count += kvs.size();
        }
        return count;
    }
}