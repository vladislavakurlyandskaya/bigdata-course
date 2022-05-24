import numpy as np
import pandas as pd
import datetime as dt

data_size = 1000


def gen_date_of_start(since, term):
    since = dt.date.fromisoformat(since)
    if term == "----------":
        term = dt.datetime.now().date()
    else:
        term = dt.date.fromisoformat(term)
    delta = term - since
    if delta.days <=31:
        return since.isoformat()
    else:
        date = since + dt.timedelta(days=np.random.randint(delta.days + 1))
        return date.isoformat()

def gen_date_of_term(since, term):
    since = dt.date.fromisoformat(since)
    if term == "----------":
        term = dt.date(year=2030, month=1, day=1)
    else:
        term = dt.date.fromisoformat(term)
    delta = term - since
    date = since + dt.timedelta(days=np.random.randint(delta.days + 1))
    if date > dt.datetime.now().date() or date <= since + dt.timedelta(days=31):
        return "----------"
    elif date > term:
        return term.isoformat()
    else:
        return date.isoformat()


def gen_status(date_of_term):
    if date_of_term == "----------":
        return "active"
    else:
        return "inactive"

def gen_distribution():
    gen=np.random.randint(1,10)
    if gen > 6:
        return "phyzical"
    else:
        return "web"

#def gen_termination(status):
    # if status=="inactive":
    #     return term_day
    # else:
    #     return "----------"

columns = ["id","customer_id", "product_id", "activation_date","termination_date",
    "status", "distribution"]

customer=pd.read_csv("customer.csv")

since=customer['customer_since'].tolist()
term=customer['termination_date'].tolist()
instance = pd.DataFrame([columns], columns=columns)
instance.to_csv("instance.csv",mode='w', index=False, header=False)
ID=0
for i in np.arange(data_size):
    id_customer=i + 1
    start=since[i]
    termination=term[i]
    for j in range(np.random.randint(1,3)):
        ID+=1
        id_product=np.random.randint(1,21)
        active_day=gen_date_of_start(start, termination)
        term_day=gen_date_of_term(active_day, termination)
        status=gen_status(term_day)
        distribution=gen_distribution()
        if id_product in [2, 6, 7, 11, 12, 13, 15, 20]:
            if id_product == 2:
                writer = pd.DataFrame([[ID, id_customer, 1, active_day, term_day, status, distribution]])
                writer.to_csv('instance.csv', mode='a', index=False, header=False)
            elif id_product == 6 or id_product == 7:
                writer = pd.DataFrame([[ID, id_customer, 5, active_day, term_day, status, distribution]])
                writer.to_csv('instance.csv', mode='a', index=False, header=False)
            elif id_product >10 and id_product < 14:
                writer = pd.DataFrame([[ID, id_customer, 10, active_day, term_day, status, distribution]])
                writer.to_csv('instance.csv', mode='a', index=False, header=False)
            elif id_product == 15:
                writer = pd.DataFrame([[ID, id_customer, 14, active_day, term_day, status, distribution]])
                writer.to_csv('instance.csv', mode='a', index=False, header=False)
            else:
                writer = pd.DataFrame([[ID, id_customer, 19, active_day, term_day, status, distribution]])
                writer.to_csv('instance.csv', mode='a', index=False, header=False)
            ID+=1
            active_day= gen_date_of_start(active_day, term_day)
            term_day=gen_date_of_term(active_day, term_day)
            status=gen_status(term_day)
            distribution=gen_distribution()
        writer = pd.DataFrame([[ID, id_customer, id_product, active_day, term_day, status, distribution]])
        writer.to_csv('instance.csv', mode='a', index=False, header=False)
