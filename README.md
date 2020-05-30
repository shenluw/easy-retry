# retry
定时扫描重试记录，实现重试功能

### 使用示例
使用内存存储记录重试
~~~java
RetryHandler<String, String, String> handler  = (group, key, s) -> {
    CompletableFuture<String> future = new CompletableFuture<>();
    // 可以启动线程执行重试逻辑
    ...
    // 执行结束设置结果
    future.complete("retry");
    // future.completeExceptionally(new Throwable("err"));
    return future;
};

Retry<String, String, String> retry = new Retry<>(new MemoryStorage(new ConcurrentHashMap<>()), handler);
retry.start();
...
// 加入重试队列
retry.push("topic1" , "some data");


// 应用关闭时
retry.shutdown();
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

// 应用关闭时
db.close();
~~~

实现了简单的 Kafka 消息重发逻辑
~~~java
KafkaTemplate<String, String>                     template;

DefaultKafkaRetryHandler<String, String>          handler  = new DefaultKafkaRetryHandler<>(template);
Retry<String, String, SendResult<String, String>> retry   = new Retry<>(handler);
...
~~~