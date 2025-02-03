package co.com.bancolombia.kafka.consumer;

import co.com.bancolombia.kafka.consumer.config.KafkaListener;
import co.com.bancolombia.kafka.consumer.config.ListenerConfig;
import co.com.bancolombia.kafka.consumer.handlers.EventsHandler;
import co.com.bancolombia.model.user.User;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
@RequiredArgsConstructor
public class KafkaConsumer {

    private final KafkaListener kafkaListener;
    private final EventsHandler eventsHandler;
    private final ReactiveKafkaConsumerTemplate<String, byte[]> kafkaConsumer;

    @EventListener(ApplicationStartedEvent.class)
    public Flux<Void> listenMessages() {
        ListenerConfig.HandlerRegistry handler = ListenerConfig.HandlerRegistry.register()
                .listen("input-topic", eventsHandler::handlerUser, User.class)
                .listen("other-topic", event -> eventsHandler.handlerObject(event).retry(3), Object.class);

        var listenerConfig = ListenerConfig.builder()
                .kafkaConsumer(kafkaConsumer)
                .handlerRegistry(handler)
                .dlq(true)
                .build();

        return kafkaListener.start(listenerConfig);
    }
}
