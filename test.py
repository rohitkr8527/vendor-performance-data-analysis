import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load .env file
load_dotenv()

USER = os.getenv("MYSQL_USER")
PASS = os.getenv("MYSQL_PASSWORD")
HOST = os.getenv("MYSQL_HOST")
DB   = os.getenv("MYSQL_DB")

print("USER:", USER)
print("PASS:", PASS)
print("HOST:", HOST)
print("DB:", DB)

# Create engine
engine = create_engine(f"mysql+pymysql://{USER}:{PASS}@{HOST}/{DB}")

with engine.connect() as conn:
    result = conn.execute(text("SELECT DATABASE();"))
    for row in result:
        print("Connected to DB:", row[0])
