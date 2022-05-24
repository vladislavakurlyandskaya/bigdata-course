import numpy as np
import pandas as pd
import datetime as dt

columns = ['charge_id', 'business_product_instance_id', 'charge_counter', 'date', 'cost', 'event_type']

instance = pd.read_csv('instance.csv')
product = pd.read_csv('product.csv')
writer = pd.DataFrame(columns=[columns])
writer.to_csv('charge.csv', index=False)
inst_number = len(instance['id'])
prod_number = len(product['product_id'])
cnt = 1
for i in np.arange(inst_number):
    inst_id = instance['id'][i]
    for j in np.arange(prod_number):
        if int(instance['id-product'][i]) == product['product_id'][j]:
            cost = product['cost'][j]
            event_type = product['recurrent'][j]
            break
    if event_type == 'charged just once':
        writer = pd.DataFrame([[cnt, inst_id, '1', instance['active-day'][i], cost, event_type]])
        writer.to_csv('charge.csv',  mode='a', index=False, header=False)
        cnt += 1
    else:
        d, m, y = [int(x) for x in instance['active-day'][i].split('-')]
        date = dt.date(y, m, d)
        if instance['status'][i] == 'active':
            term_date = dt.date(2022, 3, 1)
        else:
            d, m, y = [int(x) for x in instance['termination-day'][i].split('-')]
            term_date = dt.date(y, m, d)
        charge_counter = 1
        while date < term_date:
            tmp_date = str(date.day) + '-' + str(date.month) + '-' + str(date.year)
            writer = pd.DataFrame([[cnt, inst_id, str(charge_counter), tmp_date, cost, event_type]])
            writer.to_csv('charge.csv', mode='a', index=False, header=False)
            if date.month == 2:
                if date.year % 400 == 0 or date.year % 100 != 0 and date.year % 4 == 0:
                    delta = dt.timedelta(days=29)
                else:
                    delta = dt.timedelta(days=28)
            elif date.month in [4, 6, 9, 11]:
                delta = dt.timedelta(days=30)
            else:
                delta = dt.timedelta(days=31)
            date = date + delta
            charge_counter += 1
            cnt += 1

