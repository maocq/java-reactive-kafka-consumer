package co.com.bancolombia.kafka.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Log4j2
@Service
public class KafkaListener {
    public static final String DLQ = ".dlq";

    private final String application;
    private final ObjectMapper mapper;
    private final ReactiveKafkaProducerTemplate<String, byte[]> kafkaProducer;


    public KafkaListener(
            @Value("${spring.application.name}") String application, ObjectMapper mapper,
            ReactiveKafkaProducerTemplate<String, byte[]> kafkaProducer) {
        this.application = application;
        this.mapper = mapper;
        this.kafkaProducer = kafkaProducer;
    }

    public Flux<Void> start(ListenerConfig listener) {
        return listener.kafkaConsumer()
                .receiveAutoAck()
                .flatMap(consumerRecord ->
                            getEventListener(listener.handlerRegistry(), consumerRecord.topic())
                                .flatMap(eventListener -> {
                                    var deserialize = deserialize(consumerRecord.value(), eventListener.inputClass());
                                    return eventListener.handler().apply(deserialize);
                                })
                                .doOnError(error -> log.error("Error kafka consumer", error))
                                .doOnError(ignore -> listener.dlq(), error -> sendDlqMessage(consumerRecord, error))
                                .onErrorComplete(),
                listener.concurrency());
    }

    private Mono<ListenerConfig.EventListener<Object>> getEventListener(
            ListenerConfig.HandlerRegistry handler, String topic) {
        return handler.getListener(topic).map(Mono::just).orElseGet(Mono::empty);
    }

    @SneakyThrows
    private <T> T deserialize(byte[] data, Class<T> valueType) {
        return mapper.readValue(data, valueType);
    }

    private void sendDlqMessage(ConsumerRecord<String, byte[]> consumerRecord, Throwable error) {
        log.error("Sending DLQ message ({}) {}", consumerRecord.topic(), error.getMessage());

        List<Header> headers = List.of(
                new RecordHeader("app", application.getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("error", error.getMessage().getBytes(StandardCharsets.UTF_8)));

        var topic = consumerRecord.topic().concat(DLQ);
        var producerRecord = new ProducerRecord<String, byte[]>(
                topic, null, null, null, consumerRecord.value(), headers);
        kafkaProducer.send(producerRecord)
                .doOnError(errorDlq -> log.error("Error sending Dlq message", errorDlq))
                .onErrorComplete()
                .subscribe();
    }
}
