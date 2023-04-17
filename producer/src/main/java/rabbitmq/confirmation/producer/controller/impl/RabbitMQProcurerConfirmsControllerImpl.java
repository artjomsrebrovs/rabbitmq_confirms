package rabbitmq.confirmation.producer.controller.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import rabbitmq.confirmation.producer.controller.RabbitMQProcurerConfirmsController;
import rabbitmq.confirmation.producer.service.RabbitMQProcurerConfirmsService;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@RestController
public class RabbitMQProcurerConfirmsControllerImpl implements RabbitMQProcurerConfirmsController {

    private final RabbitMQProcurerConfirmsService rabbitMQProcurerConfirmsService;

    @Autowired
    public RabbitMQProcurerConfirmsControllerImpl(final RabbitMQProcurerConfirmsService rabbitMQProcurerConfirmsService) {
        this.rabbitMQProcurerConfirmsService = rabbitMQProcurerConfirmsService;
    }

    @Override
    @GetMapping("no_confirm")
    public ResponseEntity publishMessagesWithoutConfirms() throws IOException {
        rabbitMQProcurerConfirmsService.publishMessagesWithoutConfirms();
        return new ResponseEntity(HttpStatus.OK);
    }

    @Override
    @GetMapping("individually")
    public ResponseEntity publishMessagesIndividually() throws IOException, TimeoutException, InterruptedException {
        rabbitMQProcurerConfirmsService.publishMessagesIndividually();
        return new ResponseEntity(HttpStatus.OK);
    }

    @Override
    @GetMapping("in_batch")
    public ResponseEntity publishMessagesInBatch() throws IOException, TimeoutException, InterruptedException {
        rabbitMQProcurerConfirmsService.publishMessagesInBatch();
        return new ResponseEntity(HttpStatus.OK);
    }

    @Override
    @GetMapping("async")
    public ResponseEntity handlePublishConfirmsAsynchronously() throws IOException, InterruptedException {
        rabbitMQProcurerConfirmsService.handlePublishConfirmsAsynchronously();
        return new ResponseEntity(HttpStatus.OK);
    }
}
