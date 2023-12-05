import json

import mysql.connector
from mysql.connector import Error

data_path = "./data/"
business_file = "yelp_academic_dataset_business.json"
user_file = "yelp_academic_dataset_user.json"


def create_tables(connection):
    if connection.is_connected():
        cursor = connection.cursor()

        # Create a new database
        cursor.execute("CREATE DATABASE IF NOT EXISTS yelp_data")
        cursor.execute("USE yelp_data")

        # Create tables
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS business (
            business_id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255),
            stars FLOAT,
            review_count INT
        )
        """)

        cursor.execute("""
                CREATE TABLE IF NOT EXISTS user (
                    user_id VARCHAR(255) PRIMARY KEY,
                    review_count INT,
                    yelping_since DATETIME,
                    average_stars FLOAT
                )
                """)


def bulk_insert_users(file_path, connection, batch_size=1000):
    if connection.is_connected():
        cursor = connection.cursor()

        with open(file_path, 'r') as file:
            batch = []  # to store data for each batch

            for line in file:
                # Load each line as a JSON object
                user = json.loads(line)
                if 'user_id' not in user:
                    continue
                # Create a tuple from the JSON object
                user_data = (
                    user['user_id'],
                    user.get('review_count', None),
                    user.get('yelping_since', None),
                    user.get('average_stars', None)
                )

                # Add to batch
                batch.append(user_data)

                # Check if batch is ready to be inserted
                if len(batch) >= batch_size:
                    # Execute batch insert
                    cursor.executemany("""
                        INSERT INTO user (user_id, review_count, yelping_since, average_stars)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        review_count=VALUES(review_count), yelping_since=VALUES(yelping_since), average_stars=VALUES(average_stars)
                    """, batch)
                    connection.commit()
                    batch = []  # reset the batch

            # Insert any remaining data in the last batch
            if batch:
                cursor.executemany("""
                    INSERT INTO user (user_id, review_count, yelping_since, average_stars)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    review_count=VALUES(review_count), yelping_since=VALUES(yelping_since), average_stars=VALUES(average_stars)
                """, batch)
                connection.commit()
            cursor.close()


def bulk_insert_businesses(file_path, connection, batch_size=1000):
    if connection.is_connected():
        cursor = connection.cursor()

        with open(file_path, 'r') as file:
            batch = []  # to store data for each batch

            for line in file:
                # Load each line as a JSON object
                business = json.loads(line)

                # Create a tuple from the JSON object
                business_data = (
                    business['business_id'],
                    business['name'],
                    business.get('stars', None),  # using .get() to handle missing keys
                    business.get('review_count', None)
                )

                # Add to batch
                batch.append(business_data)

                # Check if batch is ready to be inserted
                if len(batch) >= batch_size:
                    # Execute batch insert
                    cursor.executemany("""
                                INSERT INTO business (business_id, name, stars, review_count)
                                VALUES (%s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                name=VALUES(name), stars=VALUES(stars), review_count=VALUES(review_count)
                            """, batch)
                    connection.commit()
                    batch = []  # reset the batch

            # Insert any remaining data in the last batch
            if batch:
                cursor.executemany("""
                            INSERT INTO business (business_id, name, stars, review_count)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            name=VALUES(name), stars=VALUES(stars), review_count=VALUES(review_count)
                        """, batch)
                connection.commit()
            cursor.close()


try:
    # Establish a connection to the MySQL server
    # (Replace 'localhost', 'user', and 'password' with your MySQL server's details)
    connection = mysql.connector.connect(host='localhost',
                                         user='root',
                                         password='mypassword')
    print("Creating tables...")
    create_tables(connection)
    print("Inserting businesses...")
    bulk_insert_businesses(data_path + business_file, connection, 10000)
    print("Inserting users...")
    bulk_insert_users(data_path + user_file, connection, 10000)

except Error as e:
    print(f"Error: {e}")

finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")
