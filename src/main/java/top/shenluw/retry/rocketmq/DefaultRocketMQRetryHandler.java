package top.shenluw.retry.rocketmq;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import top.shenluw.retry.RetryHandler;

import java.util.concurrent.CompletableFuture;

/**
 * @author Shenluw
 * created: 2020/5/30 21:36
 */
public class DefaultRocketMQRetryHandler<K, Data> implements RetryHandler<K, Data, SendResult> {

    private RocketMQTemplate template;

    private int timeout;

    public DefaultRocketMQRetryHandler(RocketMQTemplate template) {
        this.template = template;
    }

    @Override
    public CompletableFuture<SendResult> handle(String topic, K key, Data data) {
        int timeout;
        if (this.timeout > 0) {
            timeout = this.timeout;
        } else {
            timeout = template.getProducer().getSendMsgTimeout();
        }

        CompletableFuture<SendResult> future = new CompletableFuture<>();

        try {
            template.asyncSend(topic, data, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    future.complete(sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    future.completeExceptionally(e);
                }
            }, timeout);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    public RocketMQTemplate getTemplate() {
        return template;
    }
}
