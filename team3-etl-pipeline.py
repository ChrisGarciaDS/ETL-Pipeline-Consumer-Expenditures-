##########################################################################################################
# Import Libraries/Packages
##########################################################################################################
import os
import pymysql as mysql
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import pyodbc
import psutil
import mariadb
import sqlalchemy as sqla
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import sys
import logging.config
import logging
import time
import configparser
import sqlite3

##########################################################################################################
# Logging Configuration
##########################################################################################################
# Create a module specific new logging object for the ETL pipeline
logger = logging.getLogger(__name__)

# Capture logs at DEBUG level of lower (this includes INFO level)
# By default, logs are written to the standard console.  Here we will write all logs to the filename
logging.basicConfig(filename='etl-pipeline.log', encoding='utf-8', filemode='w', level=logging.DEBUG)

# Set the log message format
logging.basicConfig(format='%(levelname)s: %(asctime)s %(message)s', level=logging.DEBUG)


##########################################################################################################
# Extract Helper Method: extract data from Data Source 1 - Bureau of Labor Statistics Consumer Expenditures
##########################################################################################################
def extract_bls_consumer_expenditures():

    logger.info('Start Extract Session')

    try:

        # Read data source 1 - ConsumerExpenditures Database
        '''
        connection = mariadb.connect(
            user="guest",
            password="relational",
            host="relational.fit.cvut.cz",
            port=3306,
            database="ConsumerExpenditures"
        )
        '''

        # Read tables from database and copy over to local MySQL
        #conn = mysql.connect(host='localhost', port=int(3306), user='root', passwd='1234', db='ConsumerExpenditures')
        engine = sqlalchemy.create_engine("mariadb+mariadbconnector://guest:relational@relational.fit.cvut.cz:3306/ConsumerExpenditures")
        conn = engine.connect()

        sql = "SELECT * FROM EXPENDITURES;"
        df_expenditures = pd.read_sql_table('EXPENDITURES', conn)

        for col in df_expenditures.columns:
           pass


        sql = "SELECT * FROM HOUSEHOLDS;"
        #df_households = pd.read_sql_query(sql, connection)

        sql = "SELECT * FROM HOUSEHOLD_MEMBERS;"
        #df_household_members = pd.read_sql_query(sql, connection)

        # write temporary dataframe to Staging Area database
        from sqlalchemy.types import Integer, Text, String, DateTime

        database_uri = 'mysql+pymysql://temp:password123@localhost:3306/test_countries'
        local_engine = sqlalchemy.create_engine(database_uri)

        try:
            local_engine.connect()
            local_engine.execute(sql)

        except OperationalError:
            default_database_uri = 'mysql+pymysql://temp:password123@localhost:3306/ConsumerExpenditures'
            local_engine = sqlalchemy.create_engine(default_database_uri)

            with local_engine.connect() as conn:
                conn.execute("commit")
                NEW_DB_NAME = 'ConsumerExpenditures'
                local_engine.execute(f"CREATE DATABASE {NEW_DB_NAME}")

        df_expenditures.to_sql(
            "EXPENDITURES",
            con=local_engine,
            if_exists="replace",
            schema='shcema_name',
            index=False,
            chunksize=1000,
            dtype={
                "col_1_name": Integer,
                "col_2_name": Text,
                "col_3_name": String(50),
                "col_4_name": DateTime
            }
        )

    except ValueError as e:
        logger.error(e)
    logger.info("Extract Complete")


##########################################################################################################
# Helper Extract Method - Read from GDP csv & convert to database
##########################################################################################################
def extract_gdp():
    # Import CSV
    data = pd.read_csv (r'GDP.csv')   
    df = pd.DataFrame(data)

    # Connect to SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=RON\SQLEXPRESS;'
                      'Database=test_database;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()

    # Create Table
    cursor.execute('''
		    CREATE TABLE gdp (
			Date int,
			GDP(BILLIONS) int
			)
               ''')

    # Insert DataFrame to Table
    for row in df.itertuples():
        cursor.execute('''
                INSERT INTO gdp (Date, GDP(BILLIONS))
                VALUES (?,?)
                ''',
                row.Date, 
                row.GDP(BILLIONS)
                )
    conn.commit()



##########################################################################################################
# Helper Extract Method - Read from Data Source 3
##########################################################################################################
def extract_cpi():
    pass

def extract_cpi():
    logger.info('Start Extract Session')

    try:

        # Read the CSV file into a pandas DataFrame
        df_cpi = pd.read_csv('/Users/JohnnyBlaze/US_CPI.csv')

        for col in df_cpi.columns:
            pass

        # write temporary dataframe to Staging Area database
        from sqlalchemy.types import Integer, Text, String, DateTime

        database_uri = 'mysql+pymysql://temp:password123@localhost:3306/test_cpi'
        local_engine = sqlalchemy.create_engine(database_uri)

        try:
            local_engine.connect()
            local_engine.execute(sql)

        except OperationalError:
            default_database_uri = 'mysql+pymysql://temp:password123@localhost:3306/CPI'
            local_engine = sqlalchemy.create_engine(default_database_uri)

            with local_engine.connect() as conn:
                conn.execute('commit')
                NEW_DB_NAME = 'CPI'
                local_engine.execute(f"CREATE DATABASE {NEW_DB_NAME}")

        df_credit.to_sql(
            'CPI',
            con=local_engine,
            if_exists="replace",
            schema='shcema_name',
            index=False,
            chunksize=1000,
            dtype={
                "Date": DateTime,
                "Cpi": Text
            }
        )
    except Exception as e:
        logger.exception(f"Failed to Extract Data with Exception: {str(e)}")
        raise

##########################################################################################################
# Extract Method
# Read data from various data sources:
# log source file name, source file count if needed, format of the file, file size, source table name,
# source DB connection details, any exceptions/errors if file/source table missing or failed to fetch data
##########################################################################################################
def extract():
    logger.info('Start Extract Session')

    try:
        extract_bls_consumer_expenditures()
        extract_gdp()
        extract_cpi()

    except ValueError as e:
        logger.error(e)

    logger.info("Extract Complete")

##########################################################################################################
# Transform/Format/Validate the Data
##########################################################################################################
def transformation():
    logger.info('Start Transformation Session')
    pass
    logger.info("Transformation completed,data ready to load!")
    # log failed/exception messages if out of memory during processing, any data/format conversion required


def load():
    logger.info('Start Load Session')
    try:
        pass
    except Exception as e:
        logger.error(e)
    # log file/target locations, number of records loaded, constraints on any DB loads, load summary details


def main():
    # log initialized elements/components like folder location,
    # file location, server id, user id details, process job details

    start = time.time()
    ##extract
    start1 = time.time()
    extract()
    end1 = time.time() - start1
    logger.info("Extract function took : {} seconds".format(end1))


    ##transformation
    start2 = time.time()
    transformation()
    end2 = time.time() - start2
    logger.info("Transformation took : {} seconds".format(end2))

    ##load
    start3 = time.time()
    load()
    end3 = time.time() - start3
    logger.info("Load took : {} seconds".format(end3))
    end = time.time() - start
    logger.info("ETL Job took : {} seconds".format(end))
    logger.info('Session Summary')
    print("multiple threads took : {} seconds".format(end))

    # display job summary like process run time, memory usage, CPU usage,


if __name__ == "__main__":
    logger.info('ETL Process Initialized')
    main()
