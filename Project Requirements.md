# Project Spec: Stream Analytics Platform

## 1. Summary

A cloud-native, end-to-end data pipeline system that ingests real-time event streams via **Apache Kafka**, processes and transforms them using **PySpark DataFrames/SparkSQL on AWS EMR**, and orchestrates the entire workflow via **Apache Airflow**. 

---

## 2. AWS Infrastructure Architecture

All components must be deployed within the **same AWS VPC** to enable secure, low-latency communication.

### Kafka (EC2-Hosted)
- Deploy Apache Kafka on a single EC2 instance (e.g., `t3.medium` or larger).
- The EC2 instance must reside in the same VPC and subnet as the EMR cluster.
- **Security Group Rules:**
    - Inbound: Allow port `9092` (Kafka broker) from EMR security group.
    - Inbound: Allow port `22` (SSH) from your IP for setup/debugging.
    - Outbound: Allow all traffic.
- Run the provided producer scripts on this EC2 instance to generate mock data streams.

### EMR Cluster
- Provision an EMR cluster in the same VPC as the Kafka EC2 instance.
- EMR nodes must have network access to the Kafka broker on port `9092`.
- Use S3 for all input/output data storage.

### Networking Requirements
| Component | Location | Access |
|-----------|----------|--------|
| Kafka EC2 | VPC Subnet A | EMR nodes, local SSH |
| EMR Cluster | VPC Subnet A (or peered) | Kafka EC2, S3, Airflow |
| S3 Bucket | Same AWS Region | EMR, Airflow |
| Airflow | Local WSL or EC2 | AWS API access via IAM |

---

## 3. Functional Modules

### Module A: The Streaming Ingestion Layer (Kafka)

- **Objective:** Architect a fault-tolerant event streaming system to ingest raw data from multiple sources.

- **Key Deliverables:**
    - A multi-topic Kafka cluster configuration with at two distinct data streams. Examples:
        - `user_events` - User activity data (logins, page views, clicks).
        - `transaction_events` - E-commerce/financial transaction data.
    - Kafka Consumers that buffer messages for downstream Spark processing.
    - Documentation justifying the topic schema and partitioning strategy to optimize for downstream DataFrame joins.
    - Python scripts using the `Faker` library to generate and publish mock events to Kafka in real-time.
    - Run these scripts as "upstream systems" and design their pipelines to consume from these topics.

---

### Module B: The PySpark Transformation Engine (AWS EMR)

- **Objective:** Build a high-performance batch and micro-batch processing layer using DataFrames and SparkSQL, deployed to AWS EMR by default.

- **Key Deliverables:**
    - **SparkSession Factory Module:**
        - EMR-optimized memory and executor configurations.
        - Cluster-mode ready initialization.
    - **AWS EMR Infrastructure:**
        - Cluster provisioning scripts (AWS CLI or Infrastructure-as-Code).
        - EMR Step Execution via `spark-submit` with S3-based input/output paths.
    - **Multi-Stage DataFrame Transformation Pipeline:**
        - Complex Joins: inner, left, anti-join across event streams.
        - Aggregations with Window Functions: ranking, running totals, moving averages.
        - Set Operations: union, intersect, except.
        - Dynamic column manipulation: add, remove, rename, cast.
        - JSON dataset parsing and semi-structured data flattening.
    - **Performance Optimization:**
        - Partitioning and bucketing strategies for S3 output datasets.
        - Caching policies for intermediate DataFrames.
    - **Parameterization:**
        - The pipeline must support parameterized runs (dates, input paths, output paths).
        - Must execute as an EMR Step without manual intervention.

---

### Module C: The Orchestration Control Plane (Airflow)

- **Objective:** Design a robust DAG-based orchestration layer that schedules, monitors, and manages the entire pipeline on AWS EMR.

- **Key Deliverables:**
    - **Dynamic, Parameterized DAG:**
        - Kafka consumer trigger task.
        - EMR cluster creation or step submission via `EmrAddStepsOperator` (or equivalent).
        - Data validation task(s) post-processing.
    - **Task Dependencies:**
        - Proper dependency chains with error handling and retry logic.
        - SLA monitoring and alerting.
    - **AWS Integration:**
        - Airflow Connections and Hooks for AWS services.
        - Secure credential management.
    - **Monitoring:**
        - Airflow UI-accessible dashboard for pipeline health.
    - **Backfill Strategy:**
        - The DAG must support reprocessing historical date ranges on demand via EMR Steps.

---

## 4. Technical Constraints

| Constraint | Requirement |
|------------|-------------|
| **Default Execution Environment** | AWS Spark EMR (primary target) |
| **Local Development** | WSL Ubuntu with `pyspark_env` (dev/test only) |
| **Storage** | S3 for all input/output datasets |
| **Pathing** | Relative paths for code; S3 URIs for data |
| **Spark API** | DataFrame/Dataset APIs exclusively (No RDDs) |
| **Code Submission** | `spark-submit` compatible entry points |
| **Kafka Deployment** | EC2-hosted in same VPC as EMR |

---

## 5. Evaluation Criteria

| Category | Weight | Description |
|----------|--------|-------------|
| **Architecture Design** | 25% | Clarity of system design, justification of technical decisions. |
| **Code Quality** | 25% | Clean, modular, well-documented code. Proper error handling. |
| **Pipeline Functionality** | 25% | End-to-end execution: Kafka ingestion, Spark transformations, Airflow orchestration. |
| **Cloud Deployment** | 15% | Successful EMR deployment and S3 integration. |
| **Parameterization & Reusability** | 10% | Ability to run with different parameters and backfill historical data. |

---

## 6. Deliverables 

- EC2 instance with Kafka deployed (same VPC as EMR).
- Kafka topic configuration and consumer implementation.
- PySpark DataFrame transformation pipeline (local test + EMR deploy).
- AWS EMR cluster provisioning and step execution scripts.
- Airflow DAG with full orchestration logic.
- Documentation: Architecture diagram, design decisions, setup instructions.
- Demo: Live walkthrough of end-to-end pipeline execution.
 