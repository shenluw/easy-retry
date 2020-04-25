# kafka-retry
kafka 发送失败重试

### 使用示例
使用内存存储记录重试
~~~java
KafkaTemplate<String, String> kafkaTemplate;
...

MemoryStorage storage = new MemoryStorage(new ConcurrentHashMap<>());

KafkaRetry<String, String> retry = new KafkaRetry<>(storage, kafkaTemplate);

retry.start();
...
// 加入重试队列
retry.push("topic1" , "some data");
~~~
持久化存储重试记录
~~~groovy
// 添加依赖
implementation 'org.rocksdb:rocksdbjni:6.7.3'
~~~
替换storage
~~~java
RocksDBStorage db = new RocksDBStorage("any path");
db.open();
~~~