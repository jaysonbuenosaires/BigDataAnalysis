# Spark RDD Pipeline: Text Analysis & Keyword Extraction

**Laboratory Activity #1**
* **Author:** Jayson P. Buenosaires,
*             Carelyn Macabuhay,
*             Aron Jay Formento,
*             Rowena Bornilla
* **Date:** February 14, 2026
* **Subject:** Big Data / Apache Spark

---

## 📂 Project Overview
This project is a practical implementation of a **Big Data processing pipeline** using **Apache Spark RDDs (Resilient Distributed Datasets)**.

 The goal of this laboratory activity was to demonstrate the core principles of distributed computing—specifically **Lazy Evaluation**, **Immutability**, and **Partitioning**—by building a text analysis tool. The application processes unstructured raw text to extract, clean, and rank significant keywords based on frequency.

## 🎯 Objectives
* To create and execute a Spark RDD pipeline from scratch.
* To implement at least **5 distinct transformations** (e.g., `flatMap`, `filter`, `reduceByKey`).
* To demonstrate data preprocessing techniques including tokenization, normalization, and stop-word removal.
* To solve environment-specific challenges when running Spark on local Windows machines.

## 🛠️ Technology Stack
* **Language:** Python 3.x
* **Framework:** Apache Spark (PySpark)
* **Libraries:** `re` (Regular Expressions), `psutil` (System Monitoring), `time`

## ⚙️ How It Works (The Pipeline)
The script follows a standard ETL (Extract, Transform, Load) workflow:

1.  **Ingestion:** Loads raw, unstructured sentences into an RDD.
2.  **Tokenization (`flatMap`):** Splits sentences into a "bag of words" model.
3.  **Normalization (`map`):** Converts all text to lowercase to ensure case-insensitivity.
4.  **Cleaning (`map`):** Uses Regex to strip punctuation (e.g., converting "processing," to "processing").
5.  **Filtering (`filter`):** Removes common English "stop words" (the, is, and) to isolate meaningful technical terms.
6.  **Aggregation (`reduceByKey`):** Sums the occurrences of each unique keyword.
7.  **Sorting (`sortBy`):** Ranks the results in descending order.
8.  **Visualization:** Prints a formatted ASCII report of the top keywords.

## 🚀 Installation & Usage

### 1. Prerequisites
Ensure you have Python and PySpark installed.
```bash
pip install pyspark psutil
