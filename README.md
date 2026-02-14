# Apache Spark RDD Pipeline: Keyword Extraction

## 1. Project Overview
This project demonstrates the implementation of a distributed data processing pipeline using **Apache Spark RDDs (Resilient Distributed Datasets)**. The script processes unstructured text data to identify significant keywords by applying a sequence of transformations including tokenization, cleaning, filtering, and aggregation.

**Key Features:**
* **Tokenization:** Uses `flatMap` to break sentences into individual words.
* **Data Cleaning:** Implements Regex to remove punctuation and normalizes text to lowercase.
* **Stop Word Removal:** Filters out common English words (e.g., "the", "and") to isolate meaningful content.
* **Aggregation:** Uses `reduceByKey` to calculate word frequency efficiently.

## 2. Prerequisites
Before running the script, ensure you have the following installed:
* **Python 3.x**
* **Apache Spark** (via PySpark)
* **Java 8 or higher** (Required for Spark to run)

## 3. Installation
If you haven't installed the required Python libraries, run the following command in your terminal:

```bash
pip install pyspark psutil
