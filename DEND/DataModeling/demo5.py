import psycopg2

try:
    conn = psycopg2.connect("dbname='udacity' user='student' host='venus.cvbrlvpjfwkq.us-east-2.rds.amazonaws.com' password='student'")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("CREATE DATABASE studentdb;")
    cur.close()
    conn.close()

    conn = psycopg2.connect("dbname='studentdb' user='student' host='venus.cvbrlvpjfwkq.us-east-2.rds.amazonaws.com' password='student'")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS songs;")
    cur.execute("CREATE TABLE IF NOT EXISTS songs (song_title varchar, artist varchar, year int, album varchar, single boolean);")
    cur.execute("INSERT INTO songs (song_title, artist, year, album, single) \
                VALUES (%s, %s, %s, %s, %s)", \
                ("wine", "suran", 2012, "kpop", True))
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