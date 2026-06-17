.PHONY: up down verify clean seed

up:
	docker compose -f docker/compose.yaml up -d

down:
	docker compose -f docker/compose.yaml down -v

verify:
	./gradlew spotlessApply build

clean:
	./gradlew clean

seed:
	@echo "Load harness seeding not yet implemented"
