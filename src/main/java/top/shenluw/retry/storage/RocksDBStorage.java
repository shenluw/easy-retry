package top.shenluw.retry.storage;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.SerializationUtils;
import top.shenluw.retry.Storage;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author Shenluw
 * created: 2020/4/25 17:55
 */
public class RocksDBStorage implements Storage {

    private static final Logger log = LoggerFactory.getLogger(RocksDBStorage.class);

    static {
        RocksDB.loadLibrary();
    }

    private static class RKV extends Storage.KV implements Serializable {
        private static final long serialVersionUID = 5530674135687606467L;

        long hash;

        public RKV(KV kv) {
            this(kv.key, kv.value);
            this.timestamp = kv.timestamp;
        }

        public RKV(Serializable key, Serializable value) {
            super(key, value);
        }
    }

    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private RocksDB db;

    private final Map<String, ColumnFamilyHandle> handleMap = new HashMap<>();

    private String path;

    public RocksDBStorage(String path) {
        this.path = path;
    }

    @Override
    public void open() throws Exception {
        try (DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
                .setAtomicFlush(true)) {
            open(this.path, options);
        }
    }

    public synchronized void open(String path, DBOptions options) throws Exception {
        List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

        for (byte[] groupBs : RocksDB.listColumnFamilies(new Options(), path)) {
            String group = new String(groupBs);
            if (!new String(RocksDB.DEFAULT_COLUMN_FAMILY).equals(group)) {
                descriptors.add(new ColumnFamilyDescriptor(groupBs));
            }
        }

        List<ColumnFamilyHandle> handles = new ArrayList<>();

        db = RocksDB.open(options, path, descriptors, handles);
        for (ColumnFamilyHandle handle : handles) {
            this.handleMap.put(new String(handle.getName()), handle);
        }
    }


    private synchronized ColumnFamilyHandle getOrCreateColumnFamilyHandle(String group) throws RocksDBException {
        ColumnFamilyHandle handle = handleMap.get(group);
        if (handle != null) {
            return handle;
        }
        handle = db.createColumnFamily(new ColumnFamilyDescriptor(group.getBytes(UTF_8)));

        handleMap.put(group, handle);
        return handle;
    }

    @Override
    public void save(String group, Serializable key, Serializable data) {
        RKV kv = new RKV(key, data);
        kv.timestamp = System.currentTimeMillis();
        save(group, kv);
    }

    @Override
    public void save(String group, KV kv) {
        RKV rkv;
        if (kv instanceof RKV) {
            rkv = (RKV) kv;
        } else {
            rkv = new RKV(kv);
        }

        ColumnFamilyHandle handle = handleMap.get(group);
        try {
            if (handle == null) {
                handle = getOrCreateColumnFamilyHandle(group);
            }
            byte[] bytes = toValue(rkv);
            db.put(handle, generateKey(rkv, bytes), bytes);
        } catch (Exception e) {
            log.warn("save key error. key: {}, v: {}", kv.key, kv.value, e);
        }
    }

    @Override
    public KV pop(String group) {
        PersistedKV persistedKV = getFirst(group);
        if (persistedKV == null) {
            return null;
        }

        try {
            byte[] bytes = persistedKV.value;
            if (bytes == null) {
                return null;
            }

            RKV kv = null;
            try {
                kv = fromBytes(bytes);
            } catch (Exception e) {
                log.warn("convert error. group: {}, bytes: {}", group, bytes, e);
            }
            return kv;
        } finally {
            try {
                db.delete(handleMap.get(group), persistedKV.key);
            } catch (Exception e) {
                log.warn("delete key error. group: {}, key: {}, bytes: {}", group, new String(persistedKV.key), persistedKV.value, e);
            }
        }
    }

    @Override
    public KV peek(String group) {
        PersistedKV persistedKV = getFirst(group);
        if (persistedKV != null) {
            return fromBytes(persistedKV.value);
        }
        return null;
    }

    @Override
    public void delete(String group, KV kv) {
        if (kv instanceof RKV) {
            RKV rkv = (RKV) kv;
            try {
                byte[] bytes = null;
                if (rkv.hash <= 0) {
                    bytes = toValue(rkv);
                }
                db.delete(handleMap.get(group), generateKey(rkv, bytes));
            } catch (Exception e) {
                log.warn("delete key error. group: {}, key: {}, v: {}", group, kv.key, kv.value, e);
            }
        } else {
            log.warn("type {} can not delete. key: {}, v: {}", kv.getClass().getSimpleName(), kv.key, kv.value);
        }
    }

    private class PersistedKV {
        byte[] key, value;

        public PersistedKV(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    private PersistedKV getFirst(String group) {
        ColumnFamilyHandle handle = handleMap.get(group);
        if (handle == null) {
            return null;
        }

        try (RocksIterator iterator = db.newIterator(handle)) {
            iterator.seekToFirst();
            if (iterator.isValid()) {
                return new PersistedKV(iterator.key(), iterator.value());
            }
        }
        return null;
    }

    @Override
    public Set<String> groups() {
        return handleMap.keySet();
    }

    @Override
    public long count() {
        // ignore
        return 0;
    }

    protected RKV fromBytes(byte[] bs) {
        return (RKV) SerializationUtils.deserialize(bs);
    }

    protected byte[] toValue(RKV kv) {
        return SerializationUtils.serialize(kv);
    }

    protected byte[] generateKey(RKV kv, byte[] bytes) {
        // 分钟前缀
        long t = kv.timestamp / 1000 / 60;

        long hash = kv.hash;
        if (hash <= 0) {
            hash = Arrays.hashCode(bytes);
        }

        return (t + "" + hash).getBytes(UTF_8);
    }

    @Override
    public synchronized void close() {
        if (db != null) {
            db.close();
            db = null;
        }
    }
}