package rabbitmq.confirmation.producer.service;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface RabbitMQProcurerConfirmsService extends AutoCloseable {

    void publishMessagesWithoutConfirms() throws IOException;

    void publishMessagesIndividually() throws IOException, TimeoutException, InterruptedException;

    void publishMessagesInBatch() throws IOException, TimeoutException, InterruptedException;

    void handlePublishConfirmsAsynchronously() throws IOException, InterruptedException;
}
