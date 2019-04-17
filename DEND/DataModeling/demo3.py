import psycopg2

try:
    conn = psycopg2.connect("dbname='demodb' user='student@dev-venus' host='dev-venus.postgres.database.azure.com' password='student'")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS songs (song_title varchar, artist varchar, year int, album varchar, single boolean);")
    cur.execute("INSERT INTO songs (song_title, artist, year, album, single) \
                VALUES (%s, %s, %s, %s, %s)", \
                ("wine", "suran", 2012, "kpop", True))
except psycopg2.Error as e:
    print("Error: database exception")
    print(e)
finally:
    cur.close()
    conn.close()