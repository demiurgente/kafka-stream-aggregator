build:
	docker-compose up --build --force-recreate

build-agg:
	docker-compose build agg-producer

build-raw:
	docker-compose build raw-producer

sh-raw:
	docker exec -it raw-producer bash

sh-agg:
	docker exec -it agg-producer bash

run:
	docker-compose down && docker-compose up --force-recreate
