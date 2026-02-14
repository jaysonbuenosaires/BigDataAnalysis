from pyspark.sql import SparkSession
import re
import time

# --- 1. INITIALIZATION ---
# Using "local[1]" to ensure stability on Windows
print("Initializing Apache Spark...", end="\r")
spark = SparkSession.builder \
    .appName("GeneralTextPipeline") \
    .master("local[1]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")  # Hides unnecessary warnings
print("Spark Initialized Successfully!   \n")

# --- 2. INPUT DATA ---
# A paragraph about Apache Spark
raw_text = [
    "Apache Spark is a unified engine for large-scale data processing.",
    "It provides high-level APIs in Java, Scala, Python and R.",
    "Spark also supports a rich set of higher-level tools including Spark SQL,",
    "MLlib for machine learning, GraphX for graph processing,",
    "and Structured Streaming for stream processing."
]

# Create the initial RDD
rdd_text = sc.parallelize(raw_text)

# --- 3. THE PIPELINE (Processing Steps) ---
start_time = time.time() # Start timer

# Step 1: Split into words
rdd_words = rdd_text.flatMap(lambda line: line.split(" "))

# Step 2: Clean & Normalize (Regex remove punctuation, lower case)
rdd_clean = rdd_words.map(lambda word: re.sub(r'[^a-z0-9]', '', word.lower()))

# Step 3: Filter Stop Words
stop_words = {'a', 'an', 'the', 'is', 'it', 'in', 'for', 'and', 'of', 'to', 'with', ''}
rdd_keywords = rdd_clean.filter(lambda word: word not in stop_words and len(word) > 2)

# Step 4: Map to Pairs (word, 1)
rdd_pairs = rdd_keywords.map(lambda word: (word, 1))

# Step 5: Reduce (Count)
rdd_counts = rdd_pairs.reduceByKey(lambda a, b: a + b)

# Step 6: Sort (Descending)
rdd_sorted = rdd_counts.sortBy(lambda x: x[1], ascending=False)

# --- 4. ACTION (Execute) ---
results = rdd_sorted.collect()
end_time = time.time() # End timer

# --- 5. DESIGNED OUTPUT ---

def print_line(width=50):
    print("+" + "-" * (width - 2) + "+")

def print_row(col1, col2, width=50):
    # Calculate spacing for alignment
    space = width - 4 - len(str(col1)) - len(str(col2))
    print(f"| {col1}" + " " * space + f"{col2} |")

# Header
print("\n")
print_line()
print_row("SPARK RDD PIPELINE REPORT", "")
print_line()
print_row("Date:", "Feb 14, 2026")
print_row("Execution Time:", f"{round(end_time - start_time, 4)} sec")
print_line()

# Table Headers
print_row("KEYWORD DETECTED", "FREQUENCY")
print_line()

# Data Rows (Top 10 only for cleaner look)
for word, count in results:
    # Add a little visual bar for the frequency
    visual_bar = "█" * count
    print_row(f"{word.upper()}", f"{visual_bar} ({count})")

print_line()
print_row("Total Unique Words:", len(results))
print_line()
print("\n")

# Stop Spark
spark.stop()
