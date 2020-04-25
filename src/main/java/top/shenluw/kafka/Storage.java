package top.shenluw.kafka;

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

    default void save(String topic, Serializable key, Serializable data) {
        KV kv = new KV(key, data);
        kv.timestamp = System.currentTimeMillis();
        save(topic, kv);
    }

    void save(String topic, KV kv);

    /**
     * 根据topic获取记录并删除
     *
     * @param topic
     * @return
     */
    KV pop(String topic);

    /**
     * 根据topic获取记录
     *
     * @param topic
     * @return
     */
    KV peek(String topic);

    void delete(String topic, KV kv);

    Set<String> topics();

    /**
     * 全部重试记录数量
     */
    long count();

    class KV implements Serializable {
        private static final long serialVersionUID = -7268639121260695686L;

        public Serializable key, value;
        /**
         * 存入时间
         */
        public long timestamp;

        public KV(Serializable key, Serializable value) {
            this.key = key;
            this.value = value;
        }
    }
}
