NAME = data

all: build up

up-db:
	docker compose up -d

up-airflow:
	cd airflow && echo "AIRFLOW_UID=$$(id -u)" > .env
	cd airflow && docker compose up -d

build:
	docker compose build

up: up-db up-airflow

down:
	docker compose down
	cd airflow && docker compose down

re: clean all

clean:
	docker compose down --remove-orphans
	cd airflow && docker compose down --remove-orphans

fclean: clean
	docker compose down --rmi all --volumes
	cd airflow && docker compose down --rmi all --volumes

.PHONY: all build up down clean fclean re