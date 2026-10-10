from psycopg_pool import ConnectionPool
from app.config import POSTGRES_DSN

pool = ConnectionPool(POSTGRES_DSN, min_size=2, max_size=10, open=True)