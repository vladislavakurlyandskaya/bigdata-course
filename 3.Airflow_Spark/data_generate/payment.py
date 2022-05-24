import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

payment_methods = ['online wallet', 'bank card online', 'internet banking', 'sms payments', 'ATM', 'payment in stores', 'auto payment from a bank card']

start_date = datetime.datetime(2022, 1, 1, 0, 0, 0)
end_date = datetime.datetime(2022, 5, 19, 0, 0, 0)

amounts = 50*np.random.lognormal(mean=0.0, sigma=0.6, size=10000)
plt.hist(amounts, bins=25);

cust = pd.read_csv('customer.csv', usecols = ['customer_id', 'autopay_card', 'customer_since', 'termination_date'])
cust.head()

def random_date(start, end):    
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = np.random.randint(int_delta)
    return start + datetime.timedelta(seconds=random_second)


n = 20000
with open("payment.csv", mode="w") as w_file:
    payment = pd.DataFrame(columns=["payment_id", "customer_id", "payment_method", "date", "amount"])
    payment_id = int(0)
    for i in range(n):
        payment_id += 1;
        customer_id = np.random.choice(cust.customer_id)

        if (cust.iloc[customer_id - 1]['autopay_card'] != "----------"):
            payment_method = np.random.choice(payment_methods, p=[0.16, 0.20, 0.33, 0.03, 0.04, 0.04, 0.2])
        else:
            payment_method = np.random.choice(payment_methods, p=[0.18, 0.27, 0.4, 0.05, 0.05, 0.05, 0])

        cust_since = datetime.datetime.strptime(cust.iloc[customer_id - 1]['customer_since'], "%Y-%m-%d")
        if (cust.iloc[customer_id - 1]['termination_date'] != '----------'):
            term_date = datetime.datetime.strptime(cust.iloc[customer_id - 1]['termination_date'], "%Y-%m-%d")
        else:
            term_date = datetime.datetime(2022, 5, 19, 0, 0, 0)

        date = start_date
        if (cust_since >= start_date):
            date = random_date(cust_since, term_date)

        if (cust_since < start_date and term_date >= start_date):
            date = random_date(start_date, term_date)
        if (cust_since < start_date and term_date < start_date):
            payment_id -= 1;
            continue

        amount = np.random.choice(amounts).round(2)
        new_data = {"payment_id": payment_id, "customer_id": customer_id, "payment_method": payment_method,
                    "date": date, "amount": amount}
        payment = payment.append(new_data, ignore_index=True)
        payment.to_csv('payment.csv', index=False)
