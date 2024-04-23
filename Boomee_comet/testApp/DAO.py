import psycopg2
from connexion_pool import get_connection, release_connection
import time


# connection_pool = psycopg2.pool.SimpleConnectionPool(
#     1,  # Minimum connections
#     200,  # Maximum connections
#     user='boome_admin',
#     password='AzureDb13?',
#     host='boomedb.postgres.database.azure.com',
#     port='5432',
#     database='test_boome'
# )

# #This function retrieves a connection from the connection pool.
# def get_connection():
#     return connection_pool.getconn()


# #This function releases a connection back to the connection pool.
# def release_connection(conn):
#     connection_pool.putconn(conn)


# Function to insert records into the 'invoices_alim' table
def insert_record_into_invoices_alim(chunk):
    print('Processing chunk invoices...')
    # Establish a database connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        # Construct the INSERT SQL statement
        insert_query = """
            INSERT INTO invoices_alim(Line, Quantity, Description, DataCategory, AssetType, FeeType, RequestType, Variable, 
            StartDate, EndDate, ListPrice, Amount, PreOptimizationTotalAmount, OptimizationFactor, OptimizationDiscountAmount, NetAmount)
            VALUES %s;
            """

        # Generate parameterized values for each row in the chunk
        values = ','.join(
            cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row).decode('utf-8') for row in
            chunk)

        # Format the query with the parameterized values
        insert_query = insert_query % values

        # Execute the insert query
        cur.execute(insert_query)
        # Commit the transaction
        conn.commit()
        print("Inserted", len(chunk), "records")
    except Exception as e:
        # Handle exceptions during insertion
        print(f"An error occurred during insertion: {e}")
        error = {"invoices error": 'erreur d\'insertion de invoices dans la table alim'}
        return error
    finally:
        # Release the database connection
        cur.close()
        release_connection(conn)

# Function to migrate data from 'invoices_alim' to 'invoices' table
def migrate_from_invoices_alim_to_invoices(end_date, length_lines):
    # Establish a database connection
    conn = get_connection()
    # Create a cursor
    cur = conn.cursor()
    rowcount = {}
    count = 0
    sql_queries = [
        #"DELETE FROM invoices_alim_test WHERE line='Line#';",
        "DELETE FROM invoices_alim WHERE QUANTITY='' and datacategory='' and assettype='' and feetype='' and requesttype='';",
        "UPDATE invoices_alim SET line = REPLACE(line, '\"', '');",
        "UPDATE invoices_alim SET quantity = REPLACE(REPLACE(quantity, ' ', ''), '\"', '');",
        "UPDATE invoices_alim SET description = REPLACE(description, '""', '');",
        "UPDATE invoices_alim SET startdate = REPLACE(REPLACE(startdate, '=\"\"', ''), '\"', '');",
        "UPDATE invoices_alim SET startdate = REPLACE(startdate, '\"', '');",
        "UPDATE invoices_alim SET enddate = REPLACE(REPLACE(enddate, '=\"\"', ''), '\"', '');",
        "UPDATE invoices_alim SET enddate = REPLACE(enddate, '\"', '');",
        "UPDATE invoices_alim SET listprice = REPLACE(REPLACE(listprice, ' ', ''), ',', '.');",
        "UPDATE invoices_alim SET amount = REPLACE(REPLACE(amount, ' ', ''), ',', '.');",
        "UPDATE invoices_alim SET optimizationfactor = REPLACE(REPLACE(optimizationfactor, '=\"\"', ''), ',', '.');",
        "UPDATE invoices_alim SET optimizationfactor = REPLACE(optimizationfactor, ',', '.');",
        "UPDATE invoices_alim SET preoptimizationtotalamount = REPLACE(REPLACE(preoptimizationtotalamount, ' ', ''), ',', '.');",
        "UPDATE invoices_alim SET optimizationdiscountamount = REPLACE(REPLACE(optimizationdiscountamount, ' ', ''), ',', '.');",
        "UPDATE invoices_alim SET netamount = REPLACE(REPLACE(netamount, '\"', ''), ',', '.');",
        "UPDATE invoices_alim SET netamount = REPLACE(netamount, ',', '.');",
        "UPDATE invoices_alim SET netamount = REPLACE(netamount, '\"', '');",
        "UPDATE invoices_alim SET netamount = REPLACE(netamount, ' ', '');",
        "ALTER TABLE invoices_alim ALTER COLUMN line TYPE INTEGER USING line::INTEGER;",
        "ALTER TABLE invoices_alim ALTER COLUMN quantity TYPE FLOAT USING quantity::FLOAT;",
        "ALTER TABLE invoices_alim ALTER COLUMN listprice TYPE REAL USING listprice::REAL;",
        "ALTER TABLE invoices_alim ALTER COLUMN amount TYPE REAL USING amount::REAL;",
        "ALTER TABLE invoices_alim ALTER COLUMN preoptimizationtotalamount TYPE REAL USING preoptimizationtotalamount::REAL;",
        "ALTER TABLE invoices_alim ALTER COLUMN optimizationdiscountamount TYPE REAL USING optimizationdiscountamount::REAL;",
        "ALTER TABLE invoices_alim ALTER COLUMN netamount TYPE REAL USING netamount::REAL;",
        "UPDATE invoices_alim SET enddate = SUBSTRING(enddate, 4, 2) || '/' || SUBSTRING(enddate, 1, 2) || '/' || SUBSTRING(enddate, 7, 4);",
        "ALTER TABLE invoices_alim ADD COLUMN end_date DATE;",
        "UPDATE invoices_alim SET end_date = TO_DATE(enddate, 'DD/MM/YYYY');",
        "ALTER TABLE invoices_alim DROP COLUMN enddate;",
        "UPDATE invoices_alim SET startdate = SUBSTRING(startdate, 4, 2) || '/' || SUBSTRING(startdate, 1, 2) || '/' || SUBSTRING(startdate, 7, 4);",
        "ALTER TABLE invoices_alim ADD COLUMN start_date DATE;",
        "UPDATE invoices_alim SET start_date = TO_DATE(startdate, 'DD/MM/YYYY');",
        "ALTER TABLE invoices_alim DROP COLUMN startdate;",
        "INSERT INTO invoices (Quantity, Description, DataCategory, AssetType, FeeType, RequestType, Variable, ListPrice, Amount, PreOptimizationTotalAmount, OptimizationFactor, OptimizationDiscountAmount, NetAmount, end_date, start_date) SELECT Quantity, Description, DataCategory, AssetType, FeeType, RequestType, Variable, ListPrice, Amount, PreOptimizationTotalAmount, OptimizationFactor, OptimizationDiscountAmount, NetAmount, end_date, start_date FROM invoices_alim;",
        "DROP TABLE invoices_alim",
        "CREATE table invoices_alim  (Line VARCHAR,Quantity VARCHAR,Description VARCHAR,DataCategory VARCHAR,AssetType VARCHAR,FeeType VARCHAR,RequestType VARCHAR,Variable VARCHAR,StartDate VARCHAR,EndDate VARCHAR,ListPrice VARCHAR,Amount VARCHAR,PreOptimizationTotalAmount VARCHAR,OptimizationFactor VARCHAR,OptimizationDiscountAmount VARCHAR,NetAmount VARCHAR	);"
    ]
    exception_queries = []
    for query in sql_queries:
        print(query)
        try:
            cur.execute(query)
            conn.commit()
            print('rowcount', cur.rowcount)
            count = cur.rowcount
        except Exception as e:
            # Collect queries that caused exceptions
            exception_queries.append(query)
            error = {"invoices error": 'erreur de migration de fichier invoices'}
            return error
    
    res = cur.execute(f"select count(*) FROM invoices WHERE end_date = %s", [end_date])
    result = cur.fetchone()
    cur.close()
    release_connection(conn)
    # Retry executing exception queries
    for exception in exception_queries:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(exception)
        conn.commit()
        cur.close()
        release_connection(conn)
    print("********************* Migration vers INVOIVES DONE  Done ******************")


    return {'rowcounts': result[0], 'file_data_lines': length_lines}

# Function to delete 'invoices_alim' table and recreate it
def delete_all_insert_invoices():
    conn = get_connection()
    # Create a cursor
    cur = conn.cursor()
    sql_queries = [
        "DROP TABLE invoices_alim",
        "CREATE table invoices_alim  (Line VARCHAR,Quantity VARCHAR,Description VARCHAR,DataCategory VARCHAR,AssetType VARCHAR,FeeType VARCHAR,RequestType VARCHAR,Variable VARCHAR,StartDate VARCHAR,EndDate VARCHAR,ListPrice VARCHAR,Amount VARCHAR,PreOptimizationTotalAmount VARCHAR,OptimizationFactor VARCHAR,OptimizationDiscountAmount VARCHAR,NetAmount VARCHAR	);"
    ]

    for query in sql_queries:
        print(query)
        try:
            cur.execute(query)
            print("**********************")
            print("DROP & CREATE INVOICES", cur.rowcount)
        except Exception as e:
            # Handle exceptions during table operations
            print("exception delete and create table invoices", e)

    # Commit and close connections
    conn.commit()
    cur.close()
    release_connection(conn)




# Function to insert records into the 'Summary_alim_test' table
def insert_record_into_summary_alim(chunk):
    print('Processing chunk summary...')
    # Establish a database connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        # Construct the INSERT SQL statement
        insert_query = """INSERT INTO Summary_alim_test 
                (ID_BB_UNIQUE, QUALIFIER, BILLABLE_PRODUCT, TOTAL_UNIQ, CLIENT_UNIQ, 
                TOTAL_USAGE, CLIENT_USAGE, GROUP_AC, CLIENT_BO_OPT_AC, CLIENT_MAINTENANCE, CLIENT_PD) 
                VALUES %s;
                """

        # Generate parameterized values for each row in the chunk
        values = ', '.join(
                cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row).decode('utf-8') for row
                in chunk)

        # Format the query with the parameterized values
        insert_query = insert_query % values

        # Execute the insert query
        cur.execute(insert_query)
        # Commit the transaction
        conn.commit()
        print("Inserted", len(chunk), "records")
    except Exception as e:
        # Handle exceptions during insertion
        print(f"An error occurred during insertion: {e}")
        error = {"summary error": 'erreur d\'insertion de summary dans la table alim '}
        return error
    finally:
        # Release the database connection
        cur.close()
        release_connection(conn)

# Function to migrate data from 'Summary_alim_test' to 'summary' table
def migrate_from_summary_alim_to_summary(date_of_file, length_lines):
    conn = get_connection()
    # Create a cursor
    cur = conn.cursor()
    # Add a new column 'date_of_file' to the table
    cur.execute("ALTER TABLE summary_alim_test ADD COLUMN date_of_file VARCHAR(30);")
    # Update the 'date_of_file' column with the provided date
    cur.execute("UPDATE summary_alim_test SET date_of_file = %s;",[date_of_file])
    sql_queries = [
        "ALTER TABLE summary_alim_test ALTER COLUMN date_of_file type DATE USING date_of_file::DATE;",
        "INSERT INTO summary (ID_BB_UNIQUE,QUALIFIER,BILLABLE_PRODUCT,TOTAL_UNIQ,CLIENT_UNIQ,TOTAL_USAGE,CLIENT_USAGE,GROUP_AC,CLIENT_BO_OPT_AC,CLIENT_MAINTENANCE,CLIENT_PD,date_of_file) SELECT ID_BB_UNIQUE,QUALIFIER,BILLABLE_PRODUCT,TOTAL_UNIQ,CLIENT_UNIQ,TOTAL_USAGE,CLIENT_USAGE,GROUP_AC,CLIENT_BO_OPT_AC,CLIENT_MAINTENANCE,CLIENT_PD,date_of_file FROM summary_alim_test",
        "DROP TABLE summary_alim_test",
        "CREATE TABLE Summary_alim_test (ID_BB_UNIQUE VARCHAR(30),QUALIFIER VARCHAR(15),BILLABLE_PRODUCT VARCHAR(30),TOTAL_UNIQ REAL,CLIENT_UNIQ REAL,TOTAL_USAGE NUMERIC,CLIENT_USAGE NUMERIC,GROUP_AC NUMERIC,CLIENT_BO_OPT_AC NUMERIC,CLIENT_MAINTENANCE NUMERIC,CLIENT_PD NUMERIC);"
    ]
    for query in sql_queries:
        try:
            # Execute SQL queries
            cur.execute(query)
            print("nombre de MISE A JOUR ",cur.rowcount)

        except Exception as e:
            print("###################Exception" , e)
            error = {"summary error": 'erreur de migration de fichier de summary'}
            return error

    print("********************* Migration vers SUMMARY TEST Done ******************")
    res = cur.execute(f"select count(*) FROM summary WHERE date_of_file = %s", [date_of_file])
    result = cur.fetchone()
    conn.commit()
    cur.close()
    release_connection(conn)

    return {'rowcounts': result[0], 'file_data_lines': length_lines}

# Function to delete 'summary_alim_test' table and recreate it
def delete_all_insert_sum():
    conn = get_connection()
    # Create a cursor
    cur = conn.cursor()
    sql_queries = [
        "DROP TABLE summary_alim_test",
        "CREATE TABLE Summary_alim_test (ID_BB_UNIQUE VARCHAR(30),QUALIFIER VARCHAR(15),BILLABLE_PRODUCT VARCHAR(30),TOTAL_UNIQ REAL,CLIENT_UNIQ REAL,TOTAL_USAGE NUMERIC,CLIENT_USAGE NUMERIC,GROUP_AC NUMERIC,CLIENT_BO_OPT_AC NUMERIC,CLIENT_MAINTENANCE NUMERIC,CLIENT_PD NUMERIC);"
    ]

    for query in sql_queries:
        try:
            # Execute SQL queries
            cur.execute(query)
            print("**********************")
            print("DROP & CREATE SUM", cur.rowcount)
        except Exception as e:
            print("exception delete and create table summary", e)

    conn.commit()
    cur.close()
    release_connection(conn)




# Function to insert records into the 'SCHED_SUM_ALIM_test' table
def insert_record_into_sched_sum_alim(chunk):
    print('Processing chunk sched sum...')
    # Establish a database connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        # Construct the INSERT SQL statement
        insert_query = """
                        INSERT INTO SCHED_SUM_ALIM_test 
                            (ID_BB_UNIQUE, QUALIFIER,month_yr_of_most_recent_download, BILLABLE_PRODUCT,
                             NUM_OF_ACCOUNTS, TOTAL_USAGE, CLIENT_USAGE, GROUP_AC, CLIENT_BO_OPT_AC) 
                            VALUES %s;
                        """
        # Generate parameterized values for each row in the chunk
        values = ', '.join(cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s)", row).decode('utf-8') for row in chunk)

        # Format the query with the parameterized values
        insert_query = insert_query % values

        # Execute the insert query
        cur.execute(insert_query)
        # Commit the transaction
        conn.commit()
        print("Inserted", len(chunk), "records")
    except Exception as e:
        # Handle exceptions during insertion
        print(f"An error occurred during insertion: {e}")
        error = {"sched sum error": 'erreur d\'insertion de sched sum dans la table alim '}
        return error
    finally:
        # Release the database connection
        cur.close()
        release_connection(conn)

# Function to migrate data from 'sched_sum_alim_test' to 'sched_sum' table
def migrate_from_sched_sum_alim_to_sched_sum(date_of_file, length_lines):
    # Establish a database connection
    conn = get_connection()
    # Create a cursor
    cur = conn.cursor()
    # SQL queries to update and alter the table structure
    sql_queries = [
    "UPDATE sched_sum_alim_test SET num_of_accounts = REPLACE(num_of_accounts, 'N/A', '0');",
    "UPDATE sched_sum_alim_test SET total_usage = REPLACE(total_usage, 'N/A', '0');",
    "UPDATE sched_sum_alim_test SET client_usage = REPLACE(client_usage, 'N/A', '0');",
    "UPDATE sched_sum_alim_test SET group_ac = REPLACE(group_ac, 'N/A', '0');",
    "UPDATE sched_sum_alim_test SET client_bo_opt_ac = REPLACE(client_bo_opt_ac, 'N/A', '0');",
    "ALTER TABLE sched_sum_alim_test ADD COLUMN date_most_recent_down VARCHAR(30);",
    "UPDATE sched_sum_alim_test SET date_most_recent_down = TO_DATE('01/' || month_yr_of_most_recent_download, 'DD/MM/YYYY');",
    "ALTER TABLE SCHED_SUM_ALIM_test ADD COLUMN date_of_file VARCHAR(30);",
    ]
    # Execute SQL queries
    for query in sql_queries:
        try:
            cur.execute(query)
            print("nombre de MISE A JOUR ",cur.rowcount)
        except Exception as e:
            print("###################Exception" , e)
    # Update 'date_of_file' column with the provided date
    cur.execute("UPDATE SCHED_SUM_ALIM_test SET date_of_file = %s;",[date_of_file])
    # SQL queries for further updates and data migration
    sql_queries = [
        "ALTER TABLE SCHED_SUM_ALIM_test ALTER COLUMN date_of_file TYPE DATE USING date_of_file::DATE;",
        "ALTER TABLE sched_sum_alim_test ALTER COLUMN num_of_accounts TYPE INTEGER USING num_of_accounts::INTEGER;",
        "ALTER TABLE sched_sum_alim_test ALTER COLUMN total_usage TYPE INTEGER USING total_usage::INTEGER;",
        "ALTER TABLE sched_sum_alim_test ALTER COLUMN client_usage TYPE INTEGER USING client_usage::INTEGER;",
        "ALTER TABLE sched_sum_alim_test ALTER COLUMN group_ac TYPE INTEGER USING group_ac::INTEGER;",
        "ALTER TABLE sched_sum_alim_test ALTER COLUMN client_bo_opt_ac TYPE INTEGER USING client_bo_opt_ac::INTEGER;",
        "ALTER TABLE sched_sum_alim_test ALTER COLUMN date_most_recent_down TYPE DATE USING date_most_recent_down::DATE;",
        "INSERT INTO sched_sum (ID_BB_UNIQUE,QUALIFIER,MONTH_YR_OF_MOST_RECENT_DOWNLOAD,BILLABLE_PRODUCT,NUM_OF_ACCOUNTS,TOTAL_USAGE,CLIENT_USAGE,GROUP_AC,CLIENT_BO_OPT_AC,date_most_recent_down,date_of_file) SELECT ID_BB_UNIQUE,QUALIFIER,MONTH_YR_OF_MOST_RECENT_DOWNLOAD,BILLABLE_PRODUCT,NUM_OF_ACCOUNTS,TOTAL_USAGE,CLIENT_USAGE,GROUP_AC,CLIENT_BO_OPT_AC,date_most_recent_down,date_of_file FROM sched_sum_alim_test;",
        "DROP TABLE sched_sum_alim_test",
        "CREATE TABLE SCHED_SUM_ALIM_test (ID_BB_UNIQUE VARCHAR(30),QUALIFIER VARCHAR(30),MONTH_YR_OF_MOST_RECENT_DOWNLOAD VARCHAR(30),BILLABLE_PRODUCT VARCHAR(30),NUM_OF_ACCOUNTS VARCHAR(10),TOTAL_USAGE VARCHAR(10),CLIENT_USAGE VARCHAR(10),GROUP_AC VARCHAR(10),CLIENT_BO_OPT_AC VARCHAR(10));"
        ]
    for query in sql_queries:
        try:
            cur.execute(query)
            print("nombre de MISE A JOUR ",cur.rowcount)

        except Exception as e:
            print("###################Exception" , e)
            error = {"sched sum error": 'erreur de migration de fichier de sched sum'}
            return error
    print("********************* Migration vers sched sum test Done ******************")
    # Commit and close connections
    res = cur.execute(f"select count(*) FROM sched_sum WHERE date_of_file = %s", [date_of_file])
    result = cur.fetchone()
    conn.commit()
    cur.close()
    release_connection(conn)

    return {'rowcounts': result[0], 'file_data_lines': length_lines}


# Function to delete 'sched_sum_alim_test' table and recreate it
def delete_all_insert_sched_sum():
    # Establish a database connection
    conn = get_connection()
    # Create a cursor
    cur = conn.cursor()
    # SQL queries to drop and recreate the table
    sql_queries = [
        "DROP TABLE sched_sum_alim_test",
        "CREATE TABLE SCHED_SUM_ALIM_test (ID_BB_UNIQUE VARCHAR(30),QUALIFIER VARCHAR(30),MONTH_YR_OF_MOST_RECENT_DOWNLOAD VARCHAR(30),BILLABLE_PRODUCT VARCHAR(30),NUM_OF_ACCOUNTS VARCHAR(10),TOTAL_USAGE VARCHAR(10),CLIENT_USAGE VARCHAR(10),GROUP_AC VARCHAR(10),CLIENT_BO_OPT_AC VARCHAR(10));"
    ]

    for query in sql_queries:
        try:
            cur.execute(query)
            print("**********************")
            print("DROP & CREATE SCHED SUM", cur.rowcount)
        except Exception as e:
            print("exception delete and create table sched sum", e)

    # Commit and close connections
    conn.commit()
    cur.close()
    release_connection(conn)



# Function to insert a record into the 'sched_sec_alim' table
def insert_record_into_sched_sec_alim(chunk):
    print('Processing chunk sched sec...')
    # Establish a database connection
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Construct the INSERT SQL statement
        insert_query = """
                            INSERT INTO sched_sec_alim
                            (OUTPUT_ID, SECURITY, ID_BB_UNIQUE, QUALIFIER, PROCESSED_TIME, DOWNLOAD_PRODUCT,
                             BILLABLE_PRODUCT, SERVICE, CTRB_BILLING, CTRB_AC, BO_OPTIMIZED, CTRB_BAC)
                            VALUES %s;
                        """
        # Generate parameterized values for each row in the chunk
        values = ', '.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row).decode('utf-8') for row in chunk)

        # Format the query with the parameterized values
        insert_query = insert_query % values

        # Execute the insert query
        cur.execute(insert_query)
        # Commit the transaction
        conn.commit()
        print("Inserted", len(chunk), "records")
    except Exception as e:
        # Handle exceptions during insertion
        print(f"An error occurred: {e}")
        error = {"sched sec error": 'erreur d\'insertion de sched sec dans la table alim '}
        return error
    finally:
        # Release the database connection
        cur.close()
        release_connection(conn)

# Function to migrate data from 'sched_sec_alim' to 'sched_sec' table
def migrate_from_sched_sec_alim_to_sched_sec(date_of_file, length_lines):
    # Establish a database connection
    conn = get_connection()
    # Create a cursor
    cur = conn.cursor()
    rowcount = {}
    count = 0
    # Add a new column 'date_of_file' to the 'sched_sec_alim' table
    cur.execute("ALTER TABLE sched_sec_alim ADD COLUMN date_of_file VARCHAR(30);")

    # Update the 'date_of_file' column with the provided date
    cur.execute("UPDATE sched_sec_alim SET date_of_file = %s;",[date_of_file])

    # SQL queries for further updates and data migration
    sql_queries = [
        "ALTER TABLE sched_sec_alim ALTER COLUMN date_of_file type DATE USING date_of_file::DATE;",
        "INSERT INTO sched_sec(OUTPUT_ID, SECURITY, ID_BB_UNIQUE, QUALIFIER, PROCESSED_TIME, DOWNLOAD_PRODUCT, BILLABLE_PRODUCT,SERVICE, CTRB_BILLING, CTRB_AC, BO_OPTIMIZED, CTRB_BAC, date_of_file) SELECT OUTPUT_ID, SECURITY, ID_BB_UNIQUE, QUALIFIER, PROCESSED_TIME, DOWNLOAD_PRODUCT, BILLABLE_PRODUCT,SERVICE, CTRB_BILLING, CTRB_AC, BO_OPTIMIZED, CTRB_BAC, date_of_file FROM sched_sec_alim;",
        "DROP TABLE sched_sec_alim;",
        "CREATE table sched_sec_alim(OUTPUT_ID VARCHAR(30), SECURITY VARCHAR(30), ID_BB_UNIQUE VARCHAR(30),QUALIFIER VARCHAR(30),PROCESSED_TIME DATE,DOWNLOAD_PRODUCT VARCHAR(30),BILLABLE_PRODUCT VARCHAR(30),SERVICE VARCHAR(1),CTRB_BILLING VARCHAR(1),CTRB_AC VARCHAR(1),BO_OPTIMIZED VARCHAR(1),CTRB_BAC VARCHAR(1));"
    ]

    # Execute SQL queries
    for query in sql_queries:
        try:
            cur.execute(query)
            rowcount = cur.rowcount
            print("nombre de MISE A JOUR ", cur.rowcount)
        except Exception as e:
            print("###################Exception", e)
            error = {"sched sec error": 'erreur de migration de sched sec'}
            return error

    # Commit and close connections
    res = cur.execute(f"select count(*) FROM sched_sec WHERE date_of_file = %s", [date_of_file])
    result = cur.fetchone()
    conn.commit()
    cur.close()
    release_connection(conn)

    return {'rowcounts': result[0], 'file_data_lines': length_lines}

# Function to delete 'sched_sec_alim' table and recreate it
def delete_all_insert_sched_sec():
    # Establish a database connection
    conn = get_connection()
    # Create a cursor
    cur = conn.cursor()
    # SQL queries to drop and recreate the table
    sql_queries = [
        "DROP TABLE sched_sec_alim;",
        "CREATE table sched_sec_alim(OUTPUT_ID VARCHAR(30), SECURITY VARCHAR(30), ID_BB_UNIQUE VARCHAR(30),QUALIFIER VARCHAR(30),PROCESSED_TIME DATE,DOWNLOAD_PRODUCT VARCHAR(30),BILLABLE_PRODUCT VARCHAR(30),SERVICE VARCHAR(1),CTRB_BILLING VARCHAR(1),CTRB_AC VARCHAR(1),BO_OPTIMIZED VARCHAR(1),CTRB_BAC VARCHAR(1));"
    ]
    # Execute SQL queries
    for query in sql_queries:
        try:
            cur.execute(query)
            print("**********************")
            print("DROP & CREATE SCHED SEC", cur.rowcount)
        except Exception as e:
            print("exception delete and create table sched sec", e)
    # Commit and close connections
    conn.commit()
    cur.close()
    release_connection(conn)




# Function to insert records into 'usage_detail_alim' table
def insert_record_into_usage_detail_alim(chunk):
    print('Processing chunk usage detail...')
    # Get a connection from the pool
    conn = get_connection()
    try:
        # Create a cursor
        cur = conn.cursor()
        # Construct the INSERT SQL statement
        insert_query = """
                        INSERT INTO usage_detail_alim
                        (OUTPUT_ID, SECURITY,ID_BB_UNIQUE, QUALIFIER,PROCESSED_TIME,DOWNLOAD_PRODUCT,
                         BILLABLE_PRODUCT, SERVICE, CTRB_BILLING, CTRB_UNIQ, CTRB_AC, CTRB_MAIN, INIT_CHARGE,
                         CTRB_PD, CAPS, BO_OPTIMIZED
                         ) VALUES %s;
                        """
        # Generate values for the insert query
        values = ', '.join(cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row).decode('utf-8') for row in chunk)

        # Format the query with the parameterized values
        insert_query = insert_query % values

        # Execute the insert query
        cur.execute(insert_query)
        # Commit the transaction
        conn.commit()
        print("Inserted", len(chunk), "records")
    except Exception as e:
        # Handle exceptions during insertion
        error = {"usage detail error": 'erreur d\'insertion de usage detail dans la table alim '}
        return error
    finally:
        # Release the database connection
        cur.close()
        release_connection(conn)

# Function to migrate data from 'usage_detail_alim' to 'usage_detail' table
def migrate_from_usage_detail_alim_to_usage_detail(date_of_file, length_lines):
    # Establish a database connection
    conn = get_connection()
    # Create a cursor
    cur = conn.cursor()
    rowcount = {}
    count = 0

    # Add a new column 'date_of_file' to the 'usage_detail_alim' table
    cur.execute("ALTER TABLE usage_detail_alim ADD COLUMN date_of_file VARCHAR(30);")

    # Update the 'date_of_file' column with the provided date
    cur.execute("UPDATE usage_detail_alim SET date_of_file = %s;",[date_of_file])

    # SQL queries for further updates and data migration
    sql_queries = [
        "ALTER TABLE usage_detail_alim ALTER COLUMN date_of_file type DATE USING date_of_file::DATE;",
        "UPDATE usage_detail_alim SET caps = REPLACE(caps, 'N/A', ' ');",
        "INSERT INTO usage_detail (OUTPUT_ID,SECURITY,ID_BB_UNIQUE,QUALIFIER,PROCESSED_TIME,DOWNLOAD_PRODUCT,BILLABLE_PRODUCT,SERVICE,CTRB_BILLING,CTRB_UNIQ,CTRB_AC,CTRB_MAIN,INIT_CHARGE,CTRB_PD,CAPS,BO_OPTIMIZED,date_of_file) SELECT OUTPUT_ID,SECURITY,ID_BB_UNIQUE,QUALIFIER,PROCESSED_TIME,DOWNLOAD_PRODUCT,BILLABLE_PRODUCT,SERVICE,CTRB_BILLING,CTRB_UNIQ,CTRB_AC,CTRB_MAIN,INIT_CHARGE,CTRB_PD,CAPS,BO_OPTIMIZED,date_of_file FROM usage_detail_alim;",
        "DROP TABLE usage_detail_alim;",
        "CREATE TABLE usage_detail_alim(OUTPUT_ID VARCHAR(50),SECURITY VARCHAR(50),ID_BB_UNIQUE VARCHAR(30),QUALIFIER VARCHAR(30),PROCESSED_TIME DATE,DOWNLOAD_PRODUCT VARCHAR(30),BILLABLE_PRODUCT VARCHAR(30),SERVICE VARCHAR(30),CTRB_BILLING VARCHAR(30),CTRB_UNIQ VARCHAR(30),CTRB_AC VARCHAR(30),CTRB_MAIN VARCHAR(30),INIT_CHARGE VARCHAR(30),CTRB_PD VARCHAR(30),CAPS VARCHAR(30),BO_OPTIMIZED VARCHAR(30));",

    ]
    # Execute SQL queries
    for query in sql_queries:
        try:
            cur.execute(query)
            print("nombre de MISE A JOUR ", cur.rowcount)
        except Exception as e:
            # Handle exceptions during SQL execution and return error message
            print('exception', e)
            error = {"usage detail error": 'erreur de migration usage detail'}
            return error
    # Commit and close connections
    print('length_lines', length_lines)
    res = cur.execute(f"select count(*) FROM usage_detail WHERE date_of_file = %s", [date_of_file])
    result = cur.fetchone()
    print('result[0]', result[0])

    conn.commit()
    cur.close()
    release_connection(conn)
 
    return {'rowcounts': result[0], 'file_data_lines': length_lines}

# Function to delete 'usage_detail_alim' table and recreate it
def delete_all_insert_usage_detail():
    # Establish a database connection
    conn = get_connection()
    # Create a cursor
    cur = conn.cursor()
    # SQL queries to drop and recreate the table
    sql_queries = [
        "DROP TABLE usage_detail_alim;",
        "CREATE TABLE usage_detail_alim(OUTPUT_ID VARCHAR(50),SECURITY VARCHAR(50),ID_BB_UNIQUE VARCHAR(30),QUALIFIER VARCHAR(30),PROCESSED_TIME DATE,DOWNLOAD_PRODUCT VARCHAR(30),BILLABLE_PRODUCT VARCHAR(30),SERVICE VARCHAR(30),CTRB_BILLING VARCHAR(30),CTRB_UNIQ VARCHAR(30),CTRB_AC VARCHAR(30),CTRB_MAIN VARCHAR(30),INIT_CHARGE VARCHAR(30),CTRB_PD VARCHAR(30),CAPS VARCHAR(30),BO_OPTIMIZED VARCHAR(30));"
    ]
    # Execute SQL queries
    for query in sql_queries:
        try:
            cur.execute(query)
            print("**********************")
            print("DROP & CREATE USAGE DETAIL", cur.rowcount)
        except Exception as e:
            print("exception delete and create table usage detail", e)
            
    # Commit and close connections
    conn.commit()
    cur.close()
    release_connection(conn)

def main():
    print("Hello")