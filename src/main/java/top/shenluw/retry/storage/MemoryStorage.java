package top.shenluw.retry.storage;

import org.springframework.util.CollectionUtils;
import top.shenluw.retry.Storage;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * @author Shenluw
 * created: 2020/4/25 17:54
 */
public class MemoryStorage implements Storage {

    private Map<String, Queue<Storage.KV>> cache;

    public MemoryStorage(Map<String, Queue<KV>> cache) {
        this.cache = cache;
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
    public void save(String group, KV kv) {
        Queue<KV> queue = cache.getOrDefault(group, new ArrayDeque<>());
        queue.add(kv);
        cache.put(group, queue);
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