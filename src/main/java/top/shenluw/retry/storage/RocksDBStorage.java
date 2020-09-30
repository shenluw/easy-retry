package top.shenluw.retry.storage;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.SerializationUtils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.shenluw.retry.Storage;
import top.shenluw.retry.sequence.Sequence;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Shenluw
 * created: 2020/4/25 17:55
 */
public class RocksDBStorage implements Storage {

    private static final Logger log = LoggerFactory.getLogger(RocksDBStorage.class);

    static {
        RocksDB.loadLibrary();
    }

    protected static class RKV extends Storage.KV implements Serializable {
        private static final long serialVersionUID = 5530674135687606467L;

        private byte[] id;

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

    private Sequence sequence;

    private final AtomicBoolean opened = new AtomicBoolean(false);

    public RocksDBStorage(String path) {
        this(path, new Sequence());
    }

    public RocksDBStorage(String path, Sequence sequence) {
        this.path = path;
        this.sequence = sequence;
    }

    @Override
    public synchronized void open() throws Exception {
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
        opened.set(true);
    }


    private ColumnFamilyHandle getOrCreateColumnFamilyHandle(String group) throws RocksDBException {
        ColumnFamilyHandle handle = handleMap.get(group);
        if (handle != null) {
            return handle;
        }
        synchronized (handleMap) {
            handle = handleMap.get(group);
            if (handle == null) {
                handle = db.createColumnFamily(new ColumnFamilyDescriptor(group.getBytes(UTF_8)));
                handleMap.put(group, handle);
            }
        }
        return handle;
    }

    @Override
    public void save(String group, Serializable key, Serializable data) {
        if (!opened.get()) {
            log.warn("storage not opened. save group: {}, key: {}, v: {}", group, key, data);
            return;
        }

        RKV  kv        = new RKV(key, data);
        long timestamp = System.currentTimeMillis();
        kv.timestamp = timestamp;
        kv.putTimestamp = timestamp;
        save(group, kv);
    }

    @Override
    public void save(String group, KV kv) {
        if (!opened.get()) {
            long id = 0;
            if (kv instanceof RKV && ((RKV) kv).id != null) {
                id = toLong(((RKV) kv).id);
            }
            log.warn("storage not opened. save group: {}, id: {}, key: {}, v: {}", group, id, kv.key, kv.value);
            return;
        }

        RKV rkv;
        if (kv instanceof RKV) {
            rkv = (RKV) kv;
        } else {
            rkv = new RKV(kv);
        }

        try {
            ColumnFamilyHandle handle = getOrCreateColumnFamilyHandle(group);
            db.put(handle, generateKey(rkv), toValue(rkv));
        } catch (Exception e) {
            log.warn("save key error. key: {}, v: {}", kv.key, kv.value, e);
        }
    }

    @Override
    public KV pop(String group) {
        if (!opened.get()) {
            log.warn("storage not opened. pop group: {}", group);
            return null;
        }

        RKV persistedKV = getFirst(group);
        try {
            return persistedKV;
        } finally {
            try {
                if (persistedKV != null) {
                    db.delete(handleMap.get(group), persistedKV.id);
                }
            } catch (Exception e) {
                log.warn("delete key error. group: {}, id: {}, key: {}, bytes: {}",
                        group, toLong(persistedKV.id), persistedKV.key, persistedKV.value, e);
            }
        }
    }

    @Override
    public KV peek(String group) {
        if (opened.get()) {
            return getFirst(group);
        }
        log.warn("storage not opened. peek group: {}", group);
        return null;
    }

    @Override
    public void delete(String group, KV kv) {
        if (kv instanceof RKV) {
            RKV rkv = (RKV) kv;
            if (!opened.get()) {
                log.warn("storage not opened. delete group: {}, id: {}, key: {}, v: {}", group, toLong(rkv.id), kv.key, kv.value);
                return;
            }
            try {
                db.delete(handleMap.get(group), generateKey(rkv));
            } catch (Exception e) {
                log.warn("delete key error. group: {}, key: {}, v: {}", group, kv.key, kv.value, e);
            }
        } else {
            log.warn("type {} can not delete. key: {}, v: {}", kv.getClass().getSimpleName(), kv.key, kv.value);
        }
    }

    private RKV getFirst(String group) {
        ColumnFamilyHandle handle = handleMap.get(group);
        if (handle == null) {
            return null;
        }

        try (RocksIterator iterator = db.newIterator(handle)) {
            iterator.seekToFirst();
            if (iterator.isValid()) {
                byte[] bytes = iterator.value();
                if (bytes != null) {
                    try {
                        return fromBytes(bytes);
                    } catch (Exception e) {
                        log.warn("convert error. group: {}, bytes: {}", group, Hex.encodeHexString(bytes), e);
                    }
                }
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

    public long count(String group) {
        ColumnFamilyHandle handle = handleMap.get(group);
        if (handle == null) return 0;
        long count = 0;
        try (RocksIterator iterator = db.newIterator(handle)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                count++;
                iterator.next();
            }
        }
        return count;
    }

    protected RKV fromBytes(byte[] bs) {
        return (RKV) SerializationUtils.deserialize(bs);
    }

    protected byte[] toValue(RKV kv) {
        return SerializationUtils.serialize(kv);
    }

    protected byte[] generateKey(RKV kv) {
        if (kv.id == null) {
            kv.id = toBytes(sequence.nextId());
        }
        return kv.id;
    }

    @Override
    public synchronized void close() {
        if (db != null) {
            opened.set(false);
            db.close();
            db = null;
        }
    }

    @Override
    public boolean isOpen() {
        return opened.get();
    }

    private static byte[] toBytes(long v) {
        byte[] bs = new byte[8];
        bs[0] = (byte) (v >>> 56);
        bs[1] = (byte) (v >>> 48);
        bs[2] = (byte) (v >>> 40);
        bs[3] = (byte) (v >>> 32);
        bs[4] = (byte) (v >>> 24);
        bs[5] = (byte) (v >>> 16);
        bs[6] = (byte) (v >>> 8);
        bs[7] = (byte) (v);
        return bs;
    }

    private static long toLong(byte[] bs) {
        return (((long) bs[0] << 56) +
                ((long) (bs[1] & 255) << 48) +
                ((long) (bs[2] & 255) << 40) +
                ((long) (bs[3] & 255) << 32) +
                ((long) (bs[4] & 255) << 24) +
                ((bs[5] & 255) << 16) +
                ((bs[6] & 255) << 8) +
                ((bs[7] & 255)));
    }

}