package top.shenluw.retry;

import java.io.Serializable;
import java.util.Set;

/**
 * @author Shenluw
 * created: 2020/4/25 17:27
 */
public interface Storage extends AutoCloseable {
    /**
     * 开启一个存储
     */
    void open() throws Exception;

    /**
     * 关闭存储，释放资源
     */
    @Override
    void close() throws Exception;

    default void save(String group, Serializable key, Serializable data) {
        KV kv = new KV(key, data);

        long timestamp = System.currentTimeMillis();
        kv.timestamp = timestamp;
        kv.putTimestamp = timestamp;
        save(group, kv);
    }

    void save(String group, KV kv);

    /**
     * 根据group获取记录并删除
     *
     * @param group
     * @return
     */
    KV pop(String group);

    /**
     * 根据group获取记录
     *
     * @param group
     * @return
     */
    KV peek(String group);

    void delete(String group, KV kv);

    Set<String> groups();

    /**
     * 全部重试记录数量
     */
    long count();

    class KV implements Serializable {
        private static final long serialVersionUID = -7268639121260695686L;

        public Serializable key, value;
        /**
         * 存入时间
         * 第一次存入或者每次重试后存入时间
         */
        public long putTimestamp;
        /**
         * 发生时间
         */
        public long timestamp;
        /**
         * 已经重试次数
         */
        public int  retryTimes;

        public KV(Serializable key, Serializable value) {
            this.key = key;
            this.value = value;
        }
    }
}
