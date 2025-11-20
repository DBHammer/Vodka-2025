<div align="left">
  <h1>
    <img src="./resources/logo.png" width=100>
  	Vodka
  </h1>
</div>

**Vodka** is a new benchmark suite designed for evaluating Hybrid Transactional/Analytical Processing (HTAP) database systems. Built on top of the well-established TPC-C and TPC-H workloads, Vodka emphasizes performance, freshness, and synchronization latency in real-world mixed workloads.

The project extends `benchmarksql v5.0` and is distributed under the Apache-2.0 license.

This repository contains the artifact for the VLDB submitted paper `Vodka: Rethinking Benchmarking Philosophy in HTAP Systems`. The codebase here is designed to support the research presented in the paper.

## Overview

Vodka enables comprehensive benchmarking through three core metrics:

- **Performance**: Measured by throughput (TPS/QPhH) for transactional and analytical queries.
- **Freshness**: Measured by the lag in commit timestamps between the transactional data version and the analytical data version. If newly inserted records are not visible to the A-engines, we instead measure freshness using the difference between the query start time and the creation time of the latest version in the T-engines.
- **Synchronization Latency**: Measured by the time it takes for updates on the T-engines to become visible on the A-engines under weak or strong consistency models.

At present, Vodka supports benchmarking on three open-source HTAP systems, which are[OceanBase](https://www.oceanbase.com/), [TiDB](https://docs.pingcap.com/zh/tidb/stable), and [PostgreSQL Streaming Replication](https://www.postgresql.org/). More systems could be integrated easily.

## 1. Environment Preparation

### Requirements

- A **client** machine with JDK 1.8+, Apache Ivy, and Apache Ant.
- A **3-node cluster** to deploy the database.
- Optional: `ObProxy` for OceanBase, `HAProxy` for TiDB, none for PostgreSQL-SR.

### Installation

```bash
yum install -y ant java-1.8* apache-ivy git
```

### Clone and Build

```bash
git clone https://github.com/DBHammer/Vodka --recursive
cd Vodka
mvn package
```

### Environment Setup

```bash
sudo yum install -y sshpass
ssh-keygen -t rsa
# make sure that you can connect to the client and servers in root user
# copy ssh key from client to servers
./cluster_initialize.sh PROPERTIES_FILE copySshKey
# install chaosblade on servers for fault injection
./cluster_initialize.sh PROPERTIES_FILE installChaosblade
# configure system parameters for client and servers
./cluster_initialize.sh PROPERTIES_FILE configureSystem
# synchronize ntp clock in servers (change network segment first)
./cluster_initialize.sh PROPERTIES_FILE ntpSynchronize
# copy os_collector script to servers
./cluster_initialize.sh PROPERTIES_FILE copyOsCollector
```

## 2. Cluster Deployment (CentOS 7)

Refer to `deploy/readme.md` for detailed deployment scripts and configuration guidelines.

## 3. Benchmarking

### Step 1: Create Database

```sql
CREATE DATABASE Vodka;
```

### Step 2: Configure Benchmark

Edit the relevant property file under `config/` or use a template from `config/template/`. Example parameters:

```yaml
db=postgres
driver=org.postgresql.Driver
conn=jdbc:postgresql://49.52.27.33:5532/vodka1
connAP=jdbc:postgresql://49.52.27.35:5532/vodka1
user=postgres
password=
warehouses=120
loadWorkers=120
TPterminals=120
APTerminals=0
TPthreshold=0.1
resultDirectory=vodka_result/pg/my_result_%tY-%tm-%td_%tH%tM%tS
```

### Step 3: Load Data

```bash
./run/runDatabaseDestroy.sh postgres-template.properties
./run/runDatabaseBuild.sh postgres-template.properties
```

## 4. Running Different Types of Benchmarks

### A. Transactional Performance (TP-only)

Set `TPterminals > 0`, `APTerminals = 0`.

```
./runBenchmark.sh ../config/pg_tp_only.properties
```

### B. Analytical Performance (AP-only)

Set `TPterminals = 0`, `APTerminals > 0`.

```
./runBenchmark.sh ../config/pg_ap_only.properties
```

### C. HTAP Mixed Workload

Enable both TP and AP terminals and configure cross-checking parameters.

```
isHtapCheck=true
htapCheckType=3
htapCheckCrossFrequency=100
htapCheckApNum=1
htapCheckConnAp=jdbc:postgresql://49.52.27.35:5532/vodka1
htapCheckConnTp=jdbc:postgresql://49.52.27.33:5532/vodka1
htapCheckCrossQuantity=100
htapCheckQueryNumber=10
htapCheckFreshnessDataBound=3000
htapCheckFreshLagThreshold=10,100
./runBenchmark.sh ../config/pg_htap.properties
```

## 5. Report Generation

Vodka provides both runtime logs and post-hoc statistical summaries.

### Install R and Required Packages

```bash
sudo yum install -y R
install.packages('ggplot2')
install.packages('gridExtra')
install.packages('dplyr')
```

### Generate Reports

```
./generateResults.sh results/postgres/Vodka/my_result_%tY-%tm-%td_%tH%tM%tS
```

## Acknowledgements

- Logo generated via GPT-based AI tool.
- Special thanks to all contributors to the Vodka Benchmark project   :D

## Star History

<a href="https://star-history.com/#DBHammer/Vodka-2025&Date">

  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=DBHammer/Vodka-2025&type=Date&theme=dark" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=DBHammer/Vodka-2025&type=Date" />
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=DBHammer/Vodka-2025&type=Date" />
  </picture>



</a>

