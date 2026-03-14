import pandas as pd
import sqlite3

def main():
    # ==========================================
    # 1. Setup (Data Preprocessing)
    # ==========================================
    print("Loading and preprocessing data...")
    
    # Load sample data into a DataFrame
    try:
        df = pd.read_csv('suicide_data_sample.csv')
    except FileNotFoundError:
        print("Error: 'suicide_data_sample.csv' not found. Please ensure it is in the same directory.")
        return

    # Basic Operations: Selecting relevant columns
    # We will drop the 'iso_code' as 'country' is already sufficient for identification
    selected_columns = ['country', 'year', 'sex', 'age_group', 'suicide_rate']
    df = df[selected_columns]

    # Basic Operations: Cleaning (Dropping rows with missing values)
    df_cleaned = df.dropna()

    # Basic Operations: Filtering 
    # Let's filter to only include records where suicide_rate is greater than 0
    # and we exclude the 'ALL' age_group to focus on specific demographics
    df_filtered = df_cleaned[(df_cleaned['suicide_rate'] > 0) & (df_cleaned['age_group'] != 'ALL')]

    print(f"Data preprocessed. Original rows: {len(df)}, Final rows: {len(df_filtered)}\n")

    # ==========================================
    # 2. Advanced Operations (SQL & Exporting)
    # ==========================================
    print("Executing SQL queries and saving results...")
    
    # Create a temporary in-memory SQLite database
    conn = sqlite3.connect(':memory:')
    
    # Write the cleaned DataFrame to the SQL database table named 'dataset'
    df_filtered.to_sql('dataset', conn, index=False)

    # Write a complex SQL query to extract insights
    # Query: Find the top 5 countries with the highest average suicide rate across all specific age groups and years
    sql_query = """
        SELECT country, AVG(suicide_rate) AS avg_suicide_rate
        FROM dataset 
        GROUP BY country 
        ORDER BY avg_suicide_rate DESC 
        LIMIT 5
    """
    
    # Execute query and load results back into a new DataFrame
    try:
        top_countries_df = pd.read_sql_query(sql_query, conn)
        print("SQL Query Results (Top 5 Countries by Average Suicide Rate):")
        print(top_countries_df)
    except Exception as e:
        print(f"SQL Error: {e}")
        conn.close()
        return

    # Close the database connection
    conn.close()

    # ==========================================
    # 3. Writing DataFrame results to external storage
    # ==========================================
    # 1. CSV
    top_countries_df.to_csv('top_5_countries.csv', index=False)
    
    # 2. JSON
    top_countries_df.to_json('top_5_countries.json', orient='records', indent=4)
    
    # 3. TXT (Saving as a tab-separated text file)
    top_countries_df.to_csv('top_5_countries.txt', sep='\t', index=False)

    print("\nResults successfully written to top_5_countries.csv, .json, and .txt.")

if __name__ == "__main__":
    main()