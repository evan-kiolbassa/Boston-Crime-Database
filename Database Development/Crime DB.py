import psycopg2
import csv

# Establishing server connection and creating crime_db
conn = psycopg2.connect("dbname = dq user = dq")
conn.autocommit = True
cur = conn.cursor()
cur.execute("""
            CREATE DATABASE crime_db;
            """)
conn.autocommit = False
conn.close()

# Creating crimes schema for crime_db
conn = psycopg2.connect('dbname = crime_db user = dq')
cur = conn.cursor()
cur.execute("""
            CREATE SCHEMA crimes;
            """)

'''
Creating enumerated data type for day_of_week to minimize RAM allocation.
Minimized smaller intger values using smallint and float4.
VARCHAR 60 was used to minimize memory allocation of descrtiption columns,
which has a maximum string length of 58. 
'''            

cur.execute("""
            CREATE TYPE day_week AS ENUM
            ('Sunday', 'Monday', 'Tuesday', 'Wednesday',
             'Thursday', 'Friday', 'Saturday');
             
             CREATE TABLE crimes.boston_crimes (
             incident_number INTEGER PRIMARY KEY,
             offense_code SMALLINT,
             description VARCHAR(60),
             date DATE,
             day_of_the_week TEXT,
             lat FLOAT4,
             long FLOAT4
             );
             ALTER TABLE crimes.boston_crimes
             ALTER COLUMN day_of_the_week TYPE day_week
             USING day_of_the_week::day_week;
            """)
conn.commit()
with open("boston.csv") as f:
    cur.copy_expert("""
                    COPY crimes.boston_crimes FROM STDIN 
                    WITH CSV HEADER;
                    """, f)
conn.commit()

# Creating user groups and granting necessary permissions
cur.execute("""
            REVOKE ALL ON SCHEMA public FROM public;
            REVOKE ALL ON DATABASE crime_db FROM public;
            CREATE GROUP readonly NOLOGIN;
            CREATE GROUP readwrite NOLOGIN;
            GRANT CONNECT ON DATABASE crime_db TO readonly;
            GRANT CONNECT ON DATABASE crime_db TO readwrite;
            GRANT USAGE ON SCHEMA crimes TO readonly;
            GRANT USAGE ON SCHEMA crimes TO readwrite;
            GRANT SELECT ON ALL TABLES IN SCHEMA crimes 
            TO readonly;
            GRANT INSERT, SELECT, DELETE, UPDATE ON ALL TABLES 
            IN SCHEMA crimes TO readwrite;
            """)
conn.commit()

