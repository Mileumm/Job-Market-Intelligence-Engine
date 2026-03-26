NAME = data

all: build up

ingest:
	docker-compose run --rm worker python3 bronze_raw.py

visualize:
	docker-compose run --rm worker sh -c 'python3 silver_raw.py && python3 gold_raw.py'

build:
	docker compose build

up:
	docker compose up -d db

down:
	docker compose down

re: fclean all

clean:
	docker compose down --remove-orphans

fclean: clean
	docker compose down --rmi all --volumes

.PHONY: all build up down clean fclean re ingest visualize