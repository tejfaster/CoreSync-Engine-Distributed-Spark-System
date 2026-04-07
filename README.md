# 🚀 CoreSync Engine  
### Distributed Spark System Simulation

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Spark](https://img.shields.io/badge/Apache-Spark-orange)
![Architecture](https://img.shields.io/badge/System-Distributed%20Simulation-purple)
![Status](https://img.shields.io/badge/Status-Completed-success)

---

## 📌 Overview

**CoreSync Engine** is a distributed system simulation that mimics how real-world cloud infrastructures process **multi-region workloads (USA, EU)** in parallel.

Instead of using real clusters, this project simulates:
- Distributed workers  
- Dynamic resource allocation  
- Parallel Spark execution  
- Multi-layer pipelines  

👉 Built on a **single machine**, designed like a **cloud system**

---

## 🎯 What Makes This Project Special?

This is NOT just Spark usage.

It focuses on:
- System design  
- Scheduling logic  
- Resource allocation  
- Parallel execution  

👉 Spark is only the engine

---

## 🏗️ Architecture

```
                     ┌────────────────────────────┐
                     │       Controller Loop      │
                     │   (Scheduler + Allocator)  │
                     └────────────┬───────────────┘
                                  │
        ┌─────────────────────────┴─────────────────────────┐
        │                                                   │
┌───────▼────────┐                                  ┌────────▼─────────┐
│   USA Worker   │                                  │      EU Worker   │
│   (Spark Job)  │                                  │     (Spark Job)  │
└───────┬────────┘                                  └──────────┬───────┘
        │                                                      │
   ┌────▼────┐                                            ┌────▼────┐
   │ Silver  │                                            │ Silver  │
   │ Layer   │                                            │ Layer   │
   └────┬────┘                                            └────┬────┘
        │                                                      │
   ┌────▼────┐                                            ┌────▼────┐
   │ Silver  │                                            │ Silver  │
   │ Data    │                                            │ Data    │
   └────┬────┘                                            └────┬────┘
        │                      Buffer Data                     │
   ┌────▼────┐                                            ┌────▼────┐
   │ Gold    │                                            │ Gold    │
   │ Layer   │                                            │ Layer   │
   └────┬────┘                                            └────┬────┘
        │                                                      │
   ┌────▼────┐                                            ┌────▼────┐
   │ Gold    │                                            │ Gold    │
   │ Data    │                                            │ Data    │
   └────┬────┘                                            └────┬────┘
            
```

---

## ⚙️ System Configuration

- Machine: Local MacBook  
- Total CPU: 10 cores  
- Reserved: 2 cores  
- Available: 8 cores  

---

## 🔄 Execution Flow

1. Silver runs in parallel (USA + EU)  
2. Gold runs in parallel (USA + EU)  
3. Loop continues  

---

## ⚡ Dynamic Resource Allocation

| Load   | Cores |
|------  |------ |
| High   | 6–7   |
| Medium | 3–5   |
| Low    | 1–2   |

Example:
Load: 954 → 7 cores  
Load: 46 → 1 core  

---

## 📂 Data Pipeline

### 🥈 Silver Layer
Output:
data/silver/region=usa/  
data/silver/region=eu/  

---

### 🥇 Gold Layer

Runs **in parallel per region**

Reads:
```
python
df = spark.read.parquet(f"data/silver/region={region}")
```

Output:
data/gold/

---

## 📊 Observations

- Parallel execution works  
- Dynamic core allocation works  
- Gold runs twice (correct)  
- CPU usage varies with load  

---

## ⚠️ Limitations

- Single machine  
- No real cluster  
- Shared resources  

---

## 👨‍💻 Author

Tej Pratap

---

## ⭐ Final Thought

This project is about understanding distributed systems — not just using Spark.
