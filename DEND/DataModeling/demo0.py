import psycopg2

try:
    conn = psycopg2.connect("dbname='postgres' user='astronaut@dev-venus' host='dev-venus.postgres.database.azure.com' password='<redacted>'")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("CREATE USER student WITH ENCRYPTED PASSWORD 'student' CREATEDB;")
except psycopg2.Error as e:
    print("Error: database exception")
    print(e)
finally:
    cur.close()
    conn.close()
