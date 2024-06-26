from psycopg2 import pool

# Initialize the connection pool
connection_pool = pool.SimpleConnectionPool(1, 10,  # Minimum and maximum connections
                                            user="boome_admin",
                                            password="AzureDb13?",
                                            host="boomedb.postgres.database.azure.com",
                                            port="5432",
                                            database="test_boome",
                                            connect_timeout=20) # in sec


def get_connection():
    return connection_pool.getconn()


def release_connection(conn):
    connection_pool.putconn(conn)