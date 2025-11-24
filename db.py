import psycopg2
from psycopg2.extras import RealDictCursor


def build_where(where: dict | None):
    if not where:
        return "", []

    clauses = []
    values = []

    for key, val in where.items():
        clauses.append(f"{key} = %s")
        values.append(val)

    return " WHERE " + " AND ".join(clauses), values


class DB:
    def __init__(self, dsn: str, autocommit=False):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = autocommit
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)

    # -------------------------
    # INSERT
    # -------------------------
    def insert(self, table: str, data: dict):
        cols = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) RETURNING *"
        self.cur.execute(sql, list(data.values()))
        row = self.cur.fetchone()
        self.conn.commit()
        return row

    # -------------------------
    # SELECT
    # -------------------------
    def select(self, table: str, fields: list[str] | None = None,
               where: dict | None = None, limit=None, order=None):
        fields_sql = ", ".join(fields) if fields else "*"

        where_sql, params = build_where(where)

        sql = f"SELECT {fields_sql} FROM {table}{where_sql}"

        if order:
            sql += f" ORDER BY {order}"

        if limit:
            sql += f" LIMIT {limit}"

        self.cur.execute(sql, params)
        return self.cur.fetchall()

    # -------------------------
    # UPDATE
    # -------------------------
    def update(self, table: str, data: dict, where: dict):
        set_part = ", ".join([f"{k} = %s" for k in data.keys()])
        where_sql, where_vals = build_where(where)

        sql = f"UPDATE {table} SET {set_part}{where_sql} RETURNING *"

        params = list(data.values()) + where_vals

        self.cur.execute(sql, params)
        row = self.cur.fetchone()
        self.conn.commit()
        return row

    # -------------------------
    # DELETE
    # -------------------------
    def delete(self, table: str, where: dict | None = None):
        where_sql, params = build_where(where)
        sql = f"DELETE FROM {table}{where_sql} RETURNING *"
        self.cur.execute(sql, params)
        rows = self.cur.fetchall()
        self.conn.commit()
        return rows

    # -------------------------
    # RAW SQL
    # -------------------------
    def query(self, sql: str, params=None):
        self.cur.execute(sql, params or [])
        try:
            return self.cur.fetchall()
        except psycopg2.ProgrammingError:
            return None

    # -------------------------
    # CLOSE
    # -------------------------
    def close(self):
        self.cur.close()
        self.conn.close()
