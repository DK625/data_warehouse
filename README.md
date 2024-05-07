# Bizflycloud ELT Pipeline

## 1. Pre-requisites
- Linux
- Python 3.9
- Pipx

## 2. Installation
### 2.1 Install Meltano
```bash
pipx install meltano --python python3.9
```

### 2.2 Install Meltano plugins
```bash
meltano install
```

### 2.3 Install DBT
```bash
meltano invoke dbt-postgres:deps
```

## 3. Configuration
Copy `.env.example` to `.env` and fill in the values.

## 4. Usage
### 4.1 Seeding Data
You only need to seed data once.
```bash
meltano invoke dbt-postgres:seed
```

### 4.2 Run Airflow Scheduler
```bash
meltano invoke airflow scheduler
```

### 4.3 Run Airflow Webserver
```bash
meltano invoke airflow webserver --port 8080
```

### 4.4 Run DBT Docs
Generate DBT docs. You only need to run this once.
```bash
meltano invoke dbt-postgres:docs-generate
```

Serve DBT docs server
```bash
meltano invoke dbt-postgres:docs-serve --port 5000
```
