package co.com.bancolombia.kafka.consumer.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.receiver.MicrometerConsumerListener;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    public ReceiverOptions<String, byte[]> kafkaReceiverOptions(KafkaProperties kafkaProperties) throws UnknownHostException {
        MeterRegistry registry = new SimpleMeterRegistry();
        MicrometerConsumerListener consumerListener = new MicrometerConsumerListener(registry);

        kafkaProperties.setClientId(InetAddress.getLocalHost().getHostName()); // Set id based on hostname, customize here another properties
        ReceiverOptions<String, byte[]> basicReceiverOptions = ReceiverOptions.create(kafkaProperties.buildConsumerProperties());
        return basicReceiverOptions.consumerListener(consumerListener)
                .consumerProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .consumerProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, byte[]> reactiveKafkaConsumerTemplate(
            ReceiverOptions<String, byte[]> kafkaReceiverOptions,
            @Value(value = "${adapters.kafka.consumer.topic}") String topic) {
        return new ReactiveKafkaConsumerTemplate<>(kafkaReceiverOptions.subscription(List.of(topic, "other-topic")));
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, byte[]> kafkaTemplateConfig(KafkaProperties properties) {
        Map<String, Object> producerProps = properties.buildProducerProperties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new ReactiveKafkaProducerTemplate<>(SenderOptions.create(producerProps));
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
