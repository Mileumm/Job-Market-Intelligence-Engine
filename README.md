# AI-Powered Data Pipeline: Automated Job Market Intelligence

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.7+-017CEE.svg?logo=Apache%20Airflow)
![Google Gemini](https://img.shields.io/badge/Google%20Gemini-Enrichment-orange.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Storage-336791.svg?logo=Postgresql)
![Docker](https://img.shields.io/badge/Docker-Infrastructure-2496ED.svg?logo=Docker)

## Project Overview
An industrial-grade ETL pipeline designed to scrape, enrich, and analyze the job market landscape in real-time. By orchestrating **Apache Airflow**, **PostgreSQL**, and the **Google Gemini API**, this system transforms raw, unstructured LinkedIn data into structured business intelligence, featuring semantic analysis of company cultures and technical requirements.

## Architecture & Pipeline Design
The system follows a modular "Medallion Architecture" logic, ensuring data integrity and traceability at every stage:

1.  **Extraction (Bronze Layer):**
    *   Targeted scraping of LinkedIn job postings using multi-query logic.
    *   Duplicate detection & "Soft-Delete" mechanism to preserve historical trends while maintaining a clean "Active" dataset.
2.  **Enrichment (Silver Layer):**
    *   **Semantic Scraping:** Automated discovery of official company websites via Clearbit/DuckDuckGo.
    *   **IA Synthesis:** Deep-crawling of company "About Us" pages followed by Gemini-powered extraction of industry, tech team size, and public sentiment.
3.  **Refinement (Gold Layer & Vis):**
    *   Automatic extraction of Tech Stacks and Salary ranges from job descriptions using LLM batching.
    *   Generation of a dynamic, interactive SPA (Single Page Application) dashboard for stakeholder exploration.

---

## Technical Reflections & Strategic Choices

### Why Apache Airflow?
While simple cron scripts are sufficient for basic automation, this project demands a robust orchestration layer. Airflow was selected for:
*   **Directed Acyclic Graphs (DAGs):** Clear visualization of complex dependencies between extraction and AI enrichment.
*   **Retry Logic:** Mission-critical when dealing with external API rate limits and web scraping volatility.
*   **Scalability:** The ability to parallelize tasks and manage long-running AI batch processes without blocking the entire pipeline.

### The Gemini Advantage vs. Deterministic Parsing
Regex and keyword-based parsers fail to capture context. Gemini is integrated to provide:
*   **Semantic Deduplication:** Identifying if "Mobile Dev" and "iOS Engineer" roles share the same core requirements.
*   **Cultural Sentiment Analysis:** Synthesizing raw website text into a "Recruiter-Ready" summary of company maturity and technical focus.
*   **Tech Stack Extraction:** Intelligently distinguishing between "nice-to-have" skills and mandatory core competencies.

---

## Reliability & Resilience (The Fallback Pattern)
*Engineered for 99% Uptime in non-deterministic environments.*

A core feature of this repository is the **Multi-Model Fallback System** implemented in `CompanyEnricher`. Facing the reality of API quotas (429 errors) and model availability, the enrichment engine implements a cascading safety net:

```python
# Strategic cascading fallback logic
models_to_try = [
    'gemini-3-flash-preview', # Tier 1: High Intelligence
    'gemini-2.5-flash',       # Tier 2: Reliable Backup
    'gemini-3.1-flash-lite',  # Tier 3: High-Volume Workhorse
    'gemini-2.5-flash-lite'   # Tier 4: Final Safety Net
]
```

**Key Resilience Pillars:**
*   **Automatic Model Rotation:** Upon detecting `RESOURCE_EXHAUSTED` or `429` errors, the pipeline instantly switches to a higher-quota model without failing the DAG.
*   **Stateful Deduplication:** The pipeline queries the database memory BEFORE scraping, reducing external API calls by up to 70%.
*   **Graceful Degradation:** If all LLM tiers fail, the system falls back to basic metadata extraction to ensure the pipeline completes its cycle.

---

## Infrastructure
The entire environment is containerized using **Docker** and **Docker Compose**, ensuring "Write Once, Run Anywhere" portability.

*   **Airflow Stack:** Distributed via Docker (Webserver, Scheduler, Worker).
*   **Database:** Persistent PostgreSQL volume for long-term data storage.
*   **Scripts:** Isolated environment for scraping and LLM interaction, preventing dependency conflicts.

---

## Future Roadmap
*   **MLOps Integration:** Implementing **MLflow** to track LLM prompt performance and cost-per-enrichment.
*   **Data Quality (DQ):** Integrating **Great Expectations** to validate data schemas between the Bronze and Silver layers (e.g., ensuring no null Tech Stacks before dashboard generation).
*   **Real-time Alerts:** Discord/Slack notifications for high-salary job discoveries.

---

**Developed with a focus on system integrity and strategic AI application.**
*Engineer: Theo BAHIN*
