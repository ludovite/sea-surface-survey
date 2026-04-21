START  ?= 1993-01-01
END    ?= 2023-12-31

.PHONY: dev prod validate test infra-up infra-down infra-destroy dashboard help

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

dev: ## Run ingestion pipeline locally (DuckDB). Override dates: make dev START=1993-01-01 END=1993-03-31
	mkdir -p data
	bruin run . --tag ingestion --start-date "$(START)" --end-date "$(END)"

prod: ## Run pipeline on GCP (BigQuery + GCS). make prod START=... END=...
	bruin run . --environment prod --force --start-date "$(START)" --end-date "$(END)"

validate: ## Validate pipeline assets
	bruin validate

test: ## Run unit tests
	uv run pytest tests/ -v

infra-up: ## Provision GCP infrastructure (Terraform)
	cd terraform && terraform init && terraform validate && terraform apply -auto-approve

infra-down: ## Destroy BigQuery datasets only (GCS bucket is kept)
	cd terraform && terraform destroy \
	  -target=google_bigquery_dataset.raw \
	  -target=google_bigquery_dataset.staging \
	  -target=google_bigquery_dataset.mart \
	  -auto-approve

infra-destroy: ## Destroy all GCP infrastructure (BigQuery + GCS bucket)
	cd terraform && terraform destroy -auto-approve

dashboard: ## Run Streamlit dashboard locally
	cd streamlit-app && uv run streamlit run app.py
