# test_mysql_connection.py
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root:root123$@localhost:3306/cricket_analyser")
try:
    with engine.connect() as conn:
        print("Connection successful!")
except Exception as e:
    print("Connection failed:", e)