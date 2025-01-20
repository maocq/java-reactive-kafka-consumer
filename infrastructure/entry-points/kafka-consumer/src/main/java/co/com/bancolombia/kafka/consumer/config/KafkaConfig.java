package co.com.bancolombia.kafka.consumer.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.MicrometerConsumerListener;
import reactor.kafka.receiver.ReceiverOptions;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

@Configuration
public class KafkaConfig {

    @Bean
    public ReceiverOptions<String, String> kafkaReceiverOptions(
            @Value(value = "${adapters.kafka.consumer.topic}") String topic,
            KafkaProperties kafkaProperties) throws UnknownHostException {
        MeterRegistry registry = new SimpleMeterRegistry();
        MicrometerConsumerListener consumerListener = new MicrometerConsumerListener(registry);


        kafkaProperties.setClientId(InetAddress.getLocalHost().getHostName()); // Set id based on hostname, customize here another properties
        ReceiverOptions<String, String> basicReceiverOptions = ReceiverOptions.create(kafkaProperties.buildConsumerProperties());
        basicReceiverOptions.consumerListener(consumerListener);
        return basicReceiverOptions.subscription(Collections.singletonList(topic));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate(ReceiverOptions<String, String> kafkaReceiverOptions) {
        return new ReactiveKafkaConsumerTemplate<>(kafkaReceiverOptions);
    }

    /*
    @Bean
    public ReactiveKafkaProducerTemplate<String, String> kafkaTemplateConfig(KafkaProperties properties) {
        Map<String, Object> producerProps = properties.buildProducerProperties();
        SenderOptions<String, String> senderOptions = SenderOptions.create(producerProps);
        SenderOptions<String, String> senderOptionsTrx = senderOptions
                //.producerProperty(ProducerConfig.ACKS_CONFIG, "all")
                //.producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "TransactionalSend")
                ;

        return new ReactiveKafkaProducerTemplate<>(senderOptionsTrx);
    }
     */
}
