package co.com.bancolombia.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

@Log4j2
@Service
@RequiredArgsConstructor
public class KafkaConsumer {
    private final ReactiveKafkaConsumerTemplate<String, String> kafkaConsumer;
    //private final ReactiveKafkaProducerTemplate<String, String> kafkaProducer;

    @EventListener(ApplicationStartedEvent.class)
    public Flux<Void> listenMessages() {
        return kafkaConsumer
                .receive()
                //.receiveAutoAck()
                //.publishOn(Schedulers.newBoundedElastic(Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE, Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE, "kafka"))
                .flatMap(record -> handler(record)
                        .doOnSuccess(ignore -> {
                            if (!record.value().contains("n")) {
                                System.out.println("Ack: " + record.value());
                                record.receiverOffset().acknowledge();
                            }
                        })
                        .doOnError(error -> log.error("Error handler", error))
                        .onErrorComplete())
                .doOnError(error -> log.error("Error processing kafka record", error))
                //.retry()    //No necesario para ReactiveKafkaConsumerTemplate de Spring, se maneja internamente en caso de desconexión
                //.repeat()           //No dejar llegar una señal de error a este punto o matara la conexión, manejar error despues del handler
                ;
    }

    public Mono<Void> handler(ReceiverRecord<String, String> record) {
        System.out.println(record.value() + " - Topic: " + record.partition() + " - Offset " + record.receiverOffset().offset());

        if (record.value().contains("error")) {
            return Mono.error(new IllegalArgumentException("Error: " + record.value()));
        }
        return Mono.just("")
                .then();
    }

    /*
        return kafkaConsumer.receiveExactlyOnce(kafkaProducer.transactionManager())
                .flatMap(Function.identity())
                .flatMap(record -> {
                    return Mono.just("a").delayElement(Duration.ofSeconds(1))
                            .flatMap(x -> kafkaProducer.send("output-topic", record.value()))
                            .flatMap(a -> kafkaProducer.transactionManager().commit());
                }, 1)
                .cast(Object.class)
                .doOnError(error -> log.error("Error processing kafka record", error))
                .retry()
                .repeat();
     */
}
