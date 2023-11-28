clean: stop
	rm -rf data/kafka{1,2,3} && rm -rf data/zoo{1,2,3} && mkdir -p data/kafka{1,2,3} && mkdir -p data/zoo{1,2,3}
start: clean
	docker compose up -d
stop:
	docker compose down
get_kafdrop:
	wget -c https://github.com/obsidiandynamics/kafdrop/releases/download/4.0.1/kafdrop-4.0.1.jar
kafdrop: get_kafdrop
	java -jar kafdrop-4.0.1.jar --kafka.brokerConnect=127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092