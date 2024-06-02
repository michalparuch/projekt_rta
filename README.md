# projekt_rta

tworzenie topica:
kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 --create --topic stock

kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 --create --topic crypto

wyświetlania:
kafka/bin/kafka-console-consumer.sh --bootstrap-server broker:9092 --topic stock --from-beginning

kafka/bin/kafka-console-consumer.sh --bootstrap-server broker:9092 --topic crypto --from-beginning
