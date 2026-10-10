import os

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql://jobqueue:jobqueue@localhost:5432/jobqueue",
)