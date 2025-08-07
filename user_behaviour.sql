!pip install pandasql
import pandas as pd
from pandasql import sqldf

pysqldf = lambda q: sqldf(q, globals())
cards = pd.read_csv('cards_data_clean.csv')
transactions = pd.read_csv('transactions_data_new.csv')
users = pd.read_csv('users_data_new.csv')

--Credit Limit Based On Card Types
q1 = """
SELECT card_type, 
       COUNT(*) as total_cards, 
       AVG(credit_limit) as avg_credit_limit,
       MAX(credit_limit) as max_credit_limit,
       MIN(credit_limit) as min_credit_limit
FROM cards
GROUP BY card_type
ORDER BY avg_credit_limit DESC
"""
insight1 = pysqldf(q1)
insight1

--10 Merchant City with The Most Amount
q2 = """
SELECT merchant_city,
SUM(amount) as total_amount
from transactions
GROUP BY merchant_city
ORDER BY total_amount DESC
LIMIT 10;
"""
insight2 = pysqldf(q2)
insight2

--Income Amount Based On Gender
q3 = """
SELECT gender, 
       COUNT(*) as total_users, 
       AVG(per_capita_income) as avg_income
FROM users
GROUP BY gender
ORDER BY avg_income DESC
"""
insight3 = pysqldf(q3)
insight3

--Debt Based On Income per Year
q4 = """
SELECT current_age, 
       COUNT(num_credit_cards) as num_credit_cards
FROM users
GROUP BY current_age
ORDER BY current_age
"""
insight4 = pysqldf(q4)
insight4

!pip install --upgrade gspread gspread_dataframe

import gspread
from gspread_dataframe import set_with_dataframe
from google.colab import auth
auth.authenticate_user()

from google.auth import default
creds, _ = default()
gc = gspread.authorize(creds)

sh = gc.create('Insight SQL Cards Analysis')

sh.add_worksheet(title="Insight1", rows="100", cols="20")
sh.add_worksheet(title="Insight2", rows="100", cols="20")
sh.add_worksheet(title="Insight3", rows="100", cols="20")
sh.add_worksheet(title="Insight4", rows="100", cols="20")

worksheet = sh.get_worksheet(0)
set_with_dataframe(worksheet, insight1)
worksheet = sh.get_worksheet(1)
set_with_dataframe(worksheet, insight2)
worksheet = sh.get_worksheet(2)
set_with_dataframe(worksheet, insight3)
worksheet = sh.get_worksheet(3)
set_with_dataframe(worksheet, insight4)

print(sh.url)