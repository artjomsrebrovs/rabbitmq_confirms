package rabbitmq.confirmation.producer.service.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.stereotype.Service;
import rabbitmq.confirmation.producer.service.RabbitMQProcurerConfirmsService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

@Service
public class RabbitMQProcurerConfirmsServiceImpl implements RabbitMQProcurerConfirmsService {

    private static final String QUEUE_NAME = "pub_confirms_queue";

    private static final int MESSAGE_COUNT = 50_000;

    private Connection connection;

    public RabbitMQProcurerConfirmsServiceImpl() throws IOException, TimeoutException {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
        connection = factory.newConnection();
    }

    @Override
    public void publishMessagesWithoutConfirms() throws IOException {
        final Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, /*durable*/false, /*exclusive*/false, /*autoDelete*/true, /*arguments*/null);

        long start = System.nanoTime();
        for (int i = 0; i <= MESSAGE_COUNT; i++) {
            final String message = String.valueOf(i);
            channel.basicPublish(/*exchange*/"", /*routingKey*/QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));

        }
        final long end = System.nanoTime();

        System.out.format("Published %,d messages in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
    }

    @Override
    public void publishMessagesIndividually() throws IOException, TimeoutException, InterruptedException {
        final Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, /*durable*/false, /*exclusive*/false, /*autoDelete*/true, /*arguments*/null);
        channel.confirmSelect();

        final long start = System.nanoTime();
        for (int i = 0; i <= MESSAGE_COUNT; i++) {
            final String message = String.valueOf(i);
            channel.basicPublish(/*exchange*/"", /*routingKey*/QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            channel.waitForConfirmsOrDie(5_000);
        }

        final long end = System.nanoTime();

        System.out.format("Published %,d messages in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
    }

    @Override
    public void publishMessagesInBatch() throws IOException, TimeoutException, InterruptedException {
        final Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, /*durable*/false, /*exclusive*/false, /*autoDelete*/true, /*arguments*/null);
        channel.confirmSelect();

        final int batchSize = 100;
        int outstandingMessageCount = 0;

        final long start = System.nanoTime();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final String body = String.valueOf(i);
            channel.basicPublish("", QUEUE_NAME, null, body.getBytes());
            outstandingMessageCount++;

            if (outstandingMessageCount == batchSize) {
                channel.waitForConfirmsOrDie(5_000);
                outstandingMessageCount = 0;
            }
        }

        if (outstandingMessageCount > 0) {
            channel.waitForConfirmsOrDie(5_000);
        }

        final long end = System.nanoTime();
        System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
    }

    @Override
    public void handlePublishConfirmsAsynchronously() throws IOException, InterruptedException {
        final Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, /*durable*/false, /*exclusive*/false, /*autoDelete*/true, /*arguments*/null);
        channel.confirmSelect();

        final ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

        final ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
            if (multiple) {
                final ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(
                        sequenceNumber, true
                );
                confirmed.clear();
            } else {
                outstandingConfirms.remove(sequenceNumber);
            }
        };

        channel.addConfirmListener(cleanOutstandingConfirms, (sequenceNumber, multiple) -> {
            final String body = outstandingConfirms.get(sequenceNumber);
            System.err.format(
                    "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
                    body, sequenceNumber, multiple
            );
            cleanOutstandingConfirms.handle(sequenceNumber, multiple);
        });

        final long start = System.nanoTime();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final String body = String.valueOf(i);
            outstandingConfirms.put(channel.getNextPublishSeqNo(), body);
            channel.basicPublish("", QUEUE_NAME, null, body.getBytes());
        }

        if (!waitUntil(Duration.ofSeconds(60), outstandingConfirms::isEmpty)) {
            throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
        }

        long end = System.nanoTime();
        System.out.format("Published %,d messages and handled confirms asynchronously in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
    }

    private boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited = +100;
        }

        return condition.getAsBoolean();
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
