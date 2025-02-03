package co.com.bancolombia.model.user;

import lombok.Builder;

@Builder(toBuilder = true)
public record User(String name) {

    public User {
        if (name == null) {
            throw new IllegalArgumentException("Invalid name user");
        }
    }
}
