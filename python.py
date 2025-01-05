import mysql.connector
from mysql.connector import Error
import pandas as pd

# MySQL connection details
hostname = "0h9ym.h.filess.io"
database = "uday_coatbreak"
port = "3306"
username = "uday_coatbreak"
password = "b26753ef13c75268b069e29f9f159f2cea196ec9"

# File path for the CSV file
csv_file_path = "olist_order_payments_dataset.csv"

try:
    # Step 1: Connect to the MySQL database
    connection = mysql.connector.connect(
        host=hostname,
        database=database,
        user=username,
        password=password,
        port=port
    )

    if connection.is_connected():
        print("Connected to MySQL database.")
        cursor = connection.cursor()

        # Step 2: Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path)

        # Step 3: Create a table for the CSV data (if not exists)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS olist_order_payments (
            order_id VARCHAR(255),
            payment_sequential INT,
            payment_type VARCHAR(255),
            payment_installments INT,
            payment_value FLOAT
        );
        """
        cursor.execute(create_table_query)
        print("Table created or already exists.")

        # Step 4: Insert CSV data into the database
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO olist_order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value)
            VALUES (%s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, tuple(row))
        
        # Commit the transaction
        connection.commit()
        print("Data inserted successfully.")

except Error as e:
    print("Error while connecting to MySQL or inserting data:", e)
finally:
    # Step 5: Close the database connection
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed.")
