# Job Market Data Pipeline — LinkedIn Scraping & AI Enrichment

![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Airflow-3.1-017CEE.svg?logo=Apache%20Airflow)
![Google Gemini](https://img.shields.io/badge/Google%20Gemini-API-orange.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%20%7C%2016-336791.svg?logo=Postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=Docker)

## Project Overview

A batch ETL pipeline that scrapes LinkedIn job postings, enriches the data with AI-generated company profiles, and serves the results through two dashboards. The system is orchestrated by **Apache Airflow**, stores data in **PostgreSQL**, and uses the **Google Gemini API** for unstructured text analysis.

**Target market:** Data engineering roles in Quebec, Canada (configurable via Streamlit UI).

---

## Architecture & Pipeline Design

The pipeline follows a **Medallion Architecture** (Bronze → Silver → Gold) with two Airflow DAGs running every 8 hours:

```
DAG 1: linkedin_bronze_to_silver              DAG 2: enrich_companies
┌──────────────┐                              ┌───────────────────────┐
│ Setup DB     │                              │ Enrich Companies      │
│ (schema init)│                              │ (Clearbit + Gemini)   │
└──────┬───────┘                              └───────────┬───────────┘
       │                                                  │
┌──────▼───────────────┐                      ┌───────────▼───────────┐
│ Scrape LinkedIn      │                      │ Enrich Individual     │
│ (multi-query, paged) │                      │ Jobs (tech stack,     │
└──────┬───────────────┘                      │ salary via Gemini)    │
       │                                      └───────────┬───────────┘
┌──────▼───────────────┐                      ┌───────────▼───────────┐
│ Enrich Descriptions  │                      │ Generate HTML         │
│ (per-job scraping)   │                      │ Dashboard             │
└──────┬───────────────┘                      └───────────────────────┘
       │
┌──────▼───────────────┐
│ Deduplicate          │
│ (soft-delete via     │
│  window functions)   │
└──────────────────────┘
```

### Bronze Layer (`bronze_raw.py`)
- Scrapes LinkedIn's public guest job API with rotating User-Agent headers and configurable anti-ban delays.
- Loads raw job postings into `raw_jobs` using a **temp-table + `ON CONFLICT` upsert** for idempotent writes.
- Fetches individual job descriptions in a second pass for jobs where `description IS NULL`.
- Deduplicates using SQL window functions (`ROW_NUMBER() OVER(PARTITION BY ...)`) with a **soft-delete** flag (`is_active`) to preserve historical data.

### Silver Layer (`company_enrichment.py`)
- Resolves official company domains via the **Clearbit Autocomplete API**.
- Scrapes and cleans corporate website text (strips `<script>`, `<nav>`, `<footer>` tags, truncates to 4000 chars).
- Sends batches of company data to **Google Gemini** for structured extraction: industry classification, company size estimation, tech team size, and a sentiment summary.
- Extracts **tech stacks** and **salary ranges** from job descriptions via a separate Gemini batch call.

### Visualization
- **Static HTML Dashboard:** A generated SPA (served via Nginx) with company profiles, job cards, and client-side filtering by skill, workplace type, and salary disclosure.
- **Streamlit Dashboard:** A live, interactive interface connected to PostgreSQL with sidebar filters (skill, location, job category, experience level, workplace type). Includes a button to **trigger Airflow DAGs** via the REST API directly from the UI.

---

## Key Technical Decisions

### Why Airflow over Cron?
- **DAG dependency management:** Descriptions can't be enriched before scraping finishes; AI enrichment requires Bronze data to exist. Airflow enforces this ordering.
- **Built-in retry logic:** External API calls (LinkedIn, Clearbit, Gemini) fail intermittently. Airflow's `retries=1` with `retry_delay=2min` handles transient failures without custom retry loops.
- **Observability:** The Airflow UI provides task-level logging, execution history, and failure alerting out of the box.

### Why Gemini for Text Extraction?
Regex-based parsers work for structured formats, but LinkedIn job descriptions are freeform. Gemini handles:
- **Tech stack extraction:** Parsing tools/languages from unstructured prose, distinguishing requirements from nice-to-haves.
- **Company profiling:** Synthesizing raw website text into structured fields (industry, size, sentiment) that would require multiple specialized NLP models to replicate deterministically.

**Limitation acknowledged:** Gemini-generated fields like sentiment summaries are LLM interpretations, not ground-truth data. They should be treated as approximate metadata.

---

## Resilience: Multi-Model Fallback

The `CompanyEnricher` class implements a **cascading model fallback** to handle Gemini API quota exhaustion (`429 / RESOURCE_EXHAUSTED` errors):

```python
models_to_try = [
    'gemini-3-flash-preview',  # Primary: highest capability
    'gemini-2.5-flash',        # Fallback 1
    'gemini-3.1-flash-lite',   # Fallback 2: highest daily quota
    'gemini-2.5-flash-lite'    # Final fallback
]
```

**How it works:**
1. The pipeline attempts the primary model first.
2. On quota errors, it automatically rotates to the next available model without failing the Airflow task.
3. If **all models are exhausted**, the pipeline returns stub records (company name only) so the DAG completes its run and can retry enrichment on the next scheduled execution.

Additionally, the scraping tasks query existing database records **before** making external requests, skipping jobs and companies that have already been processed.

---

## Installation & Setup

### Prerequisites
- Docker & Docker Compose
- A free [Google Gemini API key](https://aistudio.google.com/app/apikey)

### Steps

1. **Configure environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env and set your GEMINI_API_KEY
   ```

2. **Launch the infrastructure:**
   ```bash
   make up
   ```
   This starts the PostgreSQL database, Metabase, Nginx, and the full Airflow stack (scheduler, worker, API server, Redis).

3. **Access the interfaces:**

   | Service | URL | Credentials |
   |---|---|---|
   | Streamlit Dashboard | [http://localhost:8501](http://localhost:8501) | — |
   | Airflow UI | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow` |
   | HTML Dashboard | [http://localhost:8000](http://localhost:8000) | — |
   | Metabase (BI) | [http://localhost:3000](http://localhost:3000) | Setup on first visit |

4. **Teardown:**
   ```bash
   make down     # Stop containers
   make fclean   # Stop + remove all images and volumes
   ```

---

## Infrastructure

The project runs across **two Docker Compose files** sharing a bridged network (`job_data_network`):

| Compose File | Services |
|---|---|
| `docker-compose.yml` (root) | PostgreSQL 15 (job data), Metabase, Nginx (static dashboard) |
| `airflow/docker-compose.yaml` | Airflow (API server, Scheduler, Worker, Triggerer, DAG Processor), PostgreSQL 16 (Airflow metadata), Redis (Celery broker), Streamlit |

Data scripts are mounted into the Airflow worker container at `/opt/airflow/scripts/` and imported directly by the DAG.

---

## Project Structure

```
.
├── Makefile                        # Build/run/clean shortcuts
├── docker-compose.yml              # DB + Metabase + Nginx
├── .env.example                    # Template for required env vars
│
├── airflow/
│   ├── docker-compose.yaml         # Full Airflow stack + Streamlit
│   ├── dags/
│   │   └── linkedin_pipeline.py    # DAG definitions + task logic
│   └── streamlit/
│       └── app.py                  # Interactive dashboard
│
└── data_script/
    ├── bronze_raw.py               # LinkedIn scraping + DB upsert
    ├── silver_raw.py               # SQL view creation (regex skill flags)
    ├── company_enrichment.py       # Clearbit + Gemini enrichment
    ├── tmp_gold_raw.py             # Gold layer view (WIP)
    ├── database_utils.py           # Shared DB engine factory
    ├── test_db.py                  # Manual connection smoke test
    ├── Dockerfile                  # Python runtime for scripts
    └── requirements.txt            # Pinned dependencies
```

---

## Known Limitations & Future Work

### Current Limitations
- **No automated tests.** `test_db.py` is a manual connectivity check, not a test suite.
- **Gold layer is minimal.** The current `gold_market_stats` view is a passthrough; no pre-computed aggregations or KPIs exist yet.
- **LLM-generated fields are approximate.** Company ratings and review counts are Gemini estimations, not sourced from review platforms.
- **Single-transaction description enrichment.** A crash mid-batch rolls back all prior updates in that run.

### Planned Improvements
- [ ] **Add pytest suite:** Unit tests for parsing logic, DB operations, and JSON sanitization.
- [ ] **Build a real Gold layer:** Aggregated views (top industries, skill demand matrix, salary distributions).
- [ ] **Data validation:** Schema checks between layers using `pandera` or SQL constraints.
- [ ] **CI/CD:** GitHub Actions for linting (`ruff`) and automated tests on push.
- [ ] **Integrate real review data:** Replace LLM-estimated ratings with Glassdoor/Indeed API data.

---

*Built by Theo BAHIN — 42 Network*
