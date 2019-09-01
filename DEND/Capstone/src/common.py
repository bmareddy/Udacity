from datetime import datetime
import psycopg2
import config

def log(msg, stdout=False, file=None):
    """
    msg: string
    simple log function
    """
    formatted_msg = "[{}]: {}".format(datetime.now(),msg)
    if stdout:
        print(formatted_msg)
    if file:
        with open(file,"a") as log_writer:
            log_writer.write(formatted_msg+"\n")

def create_redshift_connection():
    conn = psycopg2.connect(f"""
                        host={config.HOST} \
                        dbname={config.DBNAME} \
                        user={config.USER} \
                        password={config.PASSWORD} \
                        port={config.PORT}
                        """)
    cur = conn.cursor()
    return (conn, cur)

def execute_redshift_load_query(query):
    try:
        conn, cur = create_redshift_connection()
        cur.execute(query)
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error: database exception executing {query}")
        print(e)
    finally:    
        cur.close()
        conn.close()

def execute_redshift_select_query(query):
    try:
        conn, cur = create_redshift_connection()
        cur.execute(query)
        rows = cur.fetchall()
        return rows
    except psycopg2.Error as e:
        print(f"Error: database exception executing {query}")
        print(e)
    finally:
        cur.close()
        conn.close()