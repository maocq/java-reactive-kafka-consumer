# Java Reactive Kafka

```sh
docker exec -it kafka kafka-console-producer --bootstrap-server kafka:9092 --topic input-topic
{"name": "Foo"}
```

```sh
docker exec -it kafka kafka-console-producer --bootstrap-server kafka:9092 --topic other-topic
{"id": "123", "name": "Foo"}
```