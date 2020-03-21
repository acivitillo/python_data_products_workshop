# Objectives

* Intro to sql and databases
* Python & databases

# Steps

1. Create a task for yourself for this exercise
2. Get your sqlite `chinook.db` from here: https://www.sqlitetutorial.net/sqlite-sample-database/
3. Read the diagram of the page above, what do you see? Write your considerations inside your github issue
4. Run code snippet 1. Does it work? If yes or no make sure to update your github issue
5. Run code snippet 2. Does it work? If yes or now make sure to update your github issue
6. What is the difference between code snippet 1 and 2?
7. Finalize your comments in your issue and close it

# Code Snippet - SQL Only

```python 
from sqlalchemy import create_engine

path_root = os.path.abspath("")
path_db = os.path.join(root_path, "chinook.db")

engine = create_engine(f'sqlite:///{path_db}')

sql = "select count(*) from invoice_items"
results = engine.execute(sql)
results.fetchall()
```

# Code Snippet 2 - SQL + Pandas

```python 
import pandas

sql = "select count(*) from invoice_items"
df = pandas.read_sql(sql, con=engine)
df
```