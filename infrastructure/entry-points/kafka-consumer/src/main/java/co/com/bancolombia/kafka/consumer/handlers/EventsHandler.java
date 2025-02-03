package co.com.bancolombia.kafka.consumer.handlers;

import co.com.bancolombia.model.user.User;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;


@Service
public class EventsHandler {

    public Mono<Void> handlerUser(User user) {
        System.out.println("-------------");
        System.out.println("-------------");
        System.out.println("User: " + user);
        System.out.println("-------------");
        System.out.println("-------------");
        if (user.name().contains("error")) {
            return Mono.error(() -> new IllegalArgumentException("Error: " + user));
        }
        return Mono.empty();
    }


    public Mono<Void> handlerObject(Object user) {
        System.out.println("--->>> " + user);
        return Mono.empty();
    }
}
