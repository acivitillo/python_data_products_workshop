# Objectives

* Intro to sql with Python pandas

# Steps

1. Create a task for yourself
2. Run code snippet below
3. Write down your notes, commentary, questions in your github issue
4. Close github issue

# Code Snippet

```python
import pandas

sql = "select * from invoices"
df = pandas.read_sql(sql, con=engine)
df.sort_values(by=["Total"], ascending=False).head(10)
```