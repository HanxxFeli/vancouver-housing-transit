# Makefile
# Run with: make <target>

.PHONY: build run test lint clean

# Build the Docker image
build:
	docker-compose build

# Run the full pipeline
run:
	docker-compose run pipeline-run

# Run with ingestion skipped (faster for development)
run-dev:
	docker-compose run pipeline-run python src/pipeline.py --skip-ingestion

# Run tests
test:
	docker-compose run test

# Lint code locally (requires flake8, black, isort installed)
lint:
	isort src/
	black src/
	flake8 src/ --max-line-length=100 --extend-ignore=E402

# Remove generated data (useful to test a clean run)
clean-data:
	rm -rf data/silver data/gold

# Remove all data including bronze (full reset)
clean-all:
	rm -rf data/silver data/gold data/bronze

# Show running containers
ps:
	docker-compose ps