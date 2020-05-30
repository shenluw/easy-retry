package top.shenluw.retry.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import top.shenluw.retry.RetryHandler;

import java.util.concurrent.CompletableFuture;

/**
 * @author Shenluw
 * created: 2020/5/30 19:10
 */
public class DefaultKafkaRetryHandler<K, Data> implements RetryHandler<K, Data, SendResult<K, Data>> {

    private KafkaTemplate<K, Data> template;

    public DefaultKafkaRetryHandler(KafkaTemplate<K, Data> template) {
        this.template = template;
    }

    @Override
    public CompletableFuture<SendResult<K, Data>> handle(String topic, K key, Data data) {
        return template.send(topic, key, data).completable();
    }

    public KafkaTemplate<K, Data> getTemplate() {
        return template;
    }
}
