# 🚀 Crypto Intelligence Pipeline

A **production-grade, real-time data pipeline** for **cryptocurrency intelligence** — combining **market data, on-chain analytics, and sentiment analysis** into a unified, scalable system.

**This pipeline empowers hedge funds and fintech startups to detect on-chain anomalies within 3 seconds, reducing risk exposure in volatile markets**

Built with **Python, Kafka, Prefect, Docker, FastAPI, Prometheus, Grafana, and Terraform (AWS)**, this project demonstrates **enterprise-scale engineering practices**: orchestration, monitoring, APIs, and cloud provisioning.

---

## 🛠 Tech Stack

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-Event%20Streaming-231F20?logo=apachekafka&logoColor=white)
![Prefect](https://img.shields.io/badge/Prefect-Orchestration-1A2C42?logo=prefect&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerization-2496ED?logo=docker&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-REST%20API-009688?logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-336791?logo=postgresql&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Dashboards-F46800?logo=grafana&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?logo=terraform&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-RDS-FF9900?logo=amazonaws&logoColor=white)

---

## 💡 Why This Project Matters

- **Financial Intelligence**: Aggregates real-time crypto market data, blockchain transactions, and news sentiment for smarter trading and risk management.
- **Enterprise-Grade Engineering**: Orchestrated with Prefect, monitored with Prometheus/Grafana, deployed via Docker & Terraform on AWS.
- **Scalable by Design**: Kafka stream processing with exactly-once semantics, supporting large-scale, low-latency workloads.
- **API-Driven Access**: Exposes insights via a REST API (FastAPI), enabling integration with dashboards, trading systems, or analytics tools.

This isn’t a toy project — it’s an **end-to-end data platform** showcasing **cloud, orchestration, and DevOps mastery** alongside **data engineering depth**.

---

## 🌟 Core Features

- **Multi-Source Ingestion**: CoinGecko (prices), Ethereum blockchain (on-chain), RSS feeds (sentiment)
- **Stream Processing**: Apache Kafka with exactly-once semantics
- **Data Enrichment**: Transaction parsing, sentiment analysis, anomaly detection
- **Workflow Orchestration**: Prefect (retry policies, scheduling, observability)
- **Monitoring & Metrics**: Prometheus + Grafana dashboards
- **REST API**: FastAPI endpoints for querying processed insights
- **Cloud-Ready**: AWS RDS via Terraform (infrastructure as code)
- **Containerized Deployment**: Full Docker stack

---

## 🏗️ Crypto Intelligence Pipeline Architecture

```mermaid
flowchart TB
    %% === Data Sources ===
    subgraph A [Data Sources]
        A1[CoinGecko]
        A2[Ethereum Blockchain]
        A3[News RSS]
    end

    %% === Producers ===
    subgraph B [Producers]
        B1[Price Producer]
        B2[On-chain Producer]
        B3[News Producer]
    end

    %% === Kafka Topics ===
    subgraph C [Kafka]
        C1[Raw Topics]
        C2[Enriched Topics]
    end

    %% === Processors ===
    subgraph D [Processors]
        D1[On-chain Parser]
        D2[Sentiment Scorer]
        D3[Anomaly Detector]
    end

    %% === Consumers / Storers ===
    subgraph E [Consumers]
        E1[Price Consumer]
        E2[On-chain Consumer]
        E3[Sentiment Consumer]
    end

    %% === Storage ===
    subgraph F [Storage]
        F1[PostgreSQL]
        F2[AWS RDS]
    end

    %% === API & Monitoring ===
    subgraph G [API & Monitoring]
        G1[FastAPI]
        G2[Prometheus]
        G3[Grafana]
    end

    %% === Orchestration ===
    subgraph H [Orchestration]
        H1[Prefect Server]
        H2[Prefect Flows]
    end

    %% === Connections ===
    %% Data Sources -> Producers
    A1 --> B1
    A2 --> B2
    A3 --> B3

    %% Producers -> Kafka Raw Topics
    B1 --> C1
    B2 --> C1
    B3 --> C1

    %% Kafka Raw -> Processors
    C1 --> D1
    C1 --> D2
    C1 --> D3

    %% Processors -> Kafka Enriched
    D1 --> C2
    D2 --> C2
    D3 --> C2

    %% Enriched Kafka -> Consumers
    C2 --> E1
    C2 --> E2
    C2 --> E3

    %% Consumers -> Storage
    E1 --> F1
    E2 --> F1
    E3 --> F1
    F1 --> F2

    %% Storage -> API
    F1 --> G1

    %% Metrics reporting to Prometheus
    B1 -. metrics .-> G2
    B2 -. metrics .-> G2
    B3 -. metrics .-> G2
    D1 -. metrics .-> G2
    D2 -. metrics .-> G2
    D3 -. metrics .-> G2
    E1 -. metrics .-> G2
    E2 -. metrics .-> G2
    E3 -. metrics .-> G2
    G1 -. metrics .-> G2
    G2 --> G3

    %% Orchestration links
    H1 --> H2
    H2 --> B
    H2 --> D
    H2 --> E
```
---
## 🎬 Demo / Screenshots

### 1️⃣ Prefect Dashboard

Real-time workflow monitoring with retries, schedules, and status updates.

![Prefect screenshot](images/Screenshot%20(139).png)

### 2️⃣ Prometheus targets  Dashboards

![Prometheus screenshot](images/Screenshot%20(129).png)

### 3️⃣ FastAPI Docs

Interactive REST API endpoints for querying processed data.

![FastAPI screenshot](images/Screenshot%20(139).png)


## 📂 Project Structure

```bash
crypto-intelligence/
│── producers/        # Data ingestion (CoinGecko, Ethereum, News)
│── processors/       # Enrichment (sentiment, parsing, anomaly detection)
│── consumers/        # Data persistence into PostgreSQL
│── api/              # FastAPI REST API
│── flows/            # Prefect workflows
│── infra/            # Terraform & Docker configs
│── docs/             # Documentation & diagrams
│── tests/            # Unit + integration tests
└── README.md
```

---

## 🚀 Quick Start (For Developers)

### ✅ Prerequisites

- Docker & Docker Compose

- Python 3.8+

- Terraform (for AWS)

🛠 Run Locally
```bash
git clone https://github.com/Lewingtonnn/crypto-intelligence
cd crypto-intelligence-pipeline
```

### Configure environment

cp .env.example .env

### Edit with your API keys & DB credentials

### Start infrastructure
```bash
docker compose up --build
```
### 🔍 Access Services

1. Prefect UI → http://localhost:4200

2. Grafana → http://localhost:3000

3. FastAPI Docs → http://localhost:8000/docs
---

## 📈 Roadmap
1. Expand analytics to Layer 2 & cross-chain data

2. Integrate LLM-based sentiment analysis

3. Deploy via Kubernetes + Helm

---

🤝 Contributing
Pull requests and discussions welcome!

---
## 📜 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.
