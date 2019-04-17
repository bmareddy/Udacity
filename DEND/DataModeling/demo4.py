import psycopg2

try:
    conn = psycopg2.connect("dbname='demodb' user='student@dev-venus' host='dev-venus.postgres.database.azure.com' password='student'")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("SELECT * FROM songs")
    row = cur.fetchone()
    while row:
        print(row)
        row = cur.fetchone()

    cur.execute("INSERT INTO songs (song_title, artist, year, album, single) \
                VALUES (%s, %s, %s, %s, %s)", \
                ("Fall in Fall", "Vibe", 2018, "kpop", True))

    cur.execute("SELECT * FROM songs")
    rows = cur.fetchall()
    print(rows)

except psycopg2.Error as e:
    print("Error: database exception")
    print(e)
finally:
    cur.close()
    conn.close()