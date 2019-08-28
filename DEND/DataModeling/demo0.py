import psycopg2

try:
    conn = psycopg2.connect("dbname='dev' user='astronaut' host='udacity-dend-proj-dw.chbi3wa10xxr.us-west-2.redshift.amazonaws.com' password='Usa2019!' port=5439")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("CREATE USER student WITH PASSWORD 'Stud3nts';")
except psycopg2.Error as e:
    print("Error: database exception")
    print(e)
finally:
    cur.close()
    conn.close()
