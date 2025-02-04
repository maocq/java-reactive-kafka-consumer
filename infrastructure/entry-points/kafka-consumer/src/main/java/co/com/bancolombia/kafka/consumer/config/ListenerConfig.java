package co.com.bancolombia.kafka.consumer.config;

import lombok.Builder;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@Builder
public record ListenerConfig(
        ReceiverOptions<String, byte[]> receiverOptions, HandlerRegistry handlerRegistry,
        int concurrency, boolean dlq) {

    public ListenerConfig(
            ReceiverOptions<String, byte[]> receiverOptions, HandlerRegistry handlerRegistry,
            int concurrency, boolean dlq) {
        if (receiverOptions == null) {
            throw new IllegalArgumentException("Invalid receiverOptions");
        }

        this.receiverOptions = receiverOptions;
        this.handlerRegistry = handlerRegistry == null ? HandlerRegistry.register() : handlerRegistry;
        this.concurrency = concurrency <= 0 ? 1 : concurrency;
        this.dlq = dlq;
    }

    public record HandlerRegistry(Map<String, EventListener<?>> listeners) {
        public static HandlerRegistry register() {
            return new HandlerRegistry(new HashMap<>());
        }

        public <T> HandlerRegistry listen(String topic, Function<T, Mono<Void>> listener, Class<T> inputClass) {
            listeners.put(topic, new EventListener<>(listener, inputClass));
            return this;
        }

        @SuppressWarnings("unchecked")
        public <T> Optional<EventListener<T>> getListener(String topic) {
            return Optional.ofNullable((EventListener<T>) listeners.get(topic));
        }
    }

    public record EventListener<T>(Function<T, Mono<Void>> handler, Class<T> inputClass) { }
}
