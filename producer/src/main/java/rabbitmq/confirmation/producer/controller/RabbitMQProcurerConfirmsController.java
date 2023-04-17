package rabbitmq.confirmation.producer.controller;

import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface RabbitMQProcurerConfirmsController {

    ResponseEntity publishMessagesWithoutConfirms() throws IOException;

    ResponseEntity publishMessagesIndividually() throws IOException, TimeoutException, InterruptedException;

    ResponseEntity publishMessagesInBatch() throws IOException, TimeoutException, InterruptedException;

    ResponseEntity handlePublishConfirmsAsynchronously() throws IOException, InterruptedException;
}
