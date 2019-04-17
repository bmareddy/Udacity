import psycopg2

try:
    conn = psycopg2.connect("dbname='postgres' user='student@dev-venus' host='dev-venus.postgres.database.azure.com' password='student'")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("CREATE DATABASE demodb;")
except psycopg2.Error as e:
    print("Error: database exception")
    print(e)
finally:
    cur.close()
    conn.close()