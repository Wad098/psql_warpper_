from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor


class DBPool:
    def __init__(self, dsn: str, minconn=1, maxconn=10):
        self.pool = SimpleConnectionPool(minconn, maxconn, dsn)

    def get_conn(self):
        conn = self.pool.getconn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        return conn, cur

    def put_conn(self, conn):
        self.pool.putconn(conn)

    def close_all(self):
        self.pool.closeall()
