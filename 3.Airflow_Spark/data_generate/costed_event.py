import numpy as np
import pandas as pd
import datetime
import csv

data_size = 2068
customer = pd.read_csv('customer.csv')
instance = pd.read_csv('instance.csv')

def gen_roaming():
    gen = np.random.randint(1, 10)
    if gen < 9:
        return "---------"
    else:
        return "roaming"


def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = np.random.randint(int_delta)
    return start + datetime.timedelta(seconds=random_second)


def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return np.random.randint(range_start, range_end)


def gen_phone():
    country_code = np.random.randint(1, 1000)
    area_code = np.random.randint(11, 100)
    phone_str = "+" + str(country_code) + "-" + str(area_code) + "-" + str(random_with_N_digits(8))
    return phone_str


columns = ["event_id", "product_instance_id", "date", "event_type", "call_count", "sms_count", "data_count", "direction", "roaming", "calling_msisdn", "called_msisdn"]
with open(r"costed_event.csv", "w") as file:
    writer = csv.writer(file, lineterminator='\r')
    writer.writerow(columns)

ID = 0
for i in np.arange(data_size):
    with open(r'costed_event.csv', 'a') as file:
        writer = csv.writer(file, lineterminator='\r')

        id_product_instance = i + 1

        for j in range(np.random.randint(15, 20)):
            ID += 1

            start_date = datetime.datetime(2022, 2, 1, 0, 0, 0)
            term_date = datetime.datetime(2022, 5, 1, 0, 0, 0)
            date = random_date(start_date, term_date)

            event_type = np.random.choice(['call', 'sms', 'data'])
            if event_type == 'call':
                call_count = np.random.randint(1, 50)
                sms_count = 0
                data_count = 0
            if event_type == 'sms':
                call_count = 0
                sms_count = 1
                data_count = 0
            if event_type == 'data':
                call_count = 0
                sms_count = 0
                data_count = np.random.randint(1, 200)

            roaming = gen_roaming()

            if (event_type == 'call' or event_type == 'sms'):

                direction = np.random.choice(['outgoing', 'incoming'])

                gen = np.random.randint(1, 15)
                if direction == 'outgoing':
                    customerId = instance.iloc[id_product_instance - 1]['customer_id']
                    calling_msisdn = customer.iloc[customerId - 1]['msisdn']
                    if gen < 14:
                        customer2Id = np.random.choice(customer.customer_id)
                        called_msisdn = customer.iloc[customer2Id - 1]['msisdn']
                    else:
                        called_msisdn = gen_phone()

                if direction == 'incoming':
                    if gen < 14:
                        customerId = np.random.choice(customer.customer_id)
                        calling_msisdn = customer.iloc[customerId - 1]['msisdn']
                    else:
                        calling_msisdn = gen_phone()
                    customer2Id = instance.iloc[id_product_instance - 1]['customer_id']
                    called_msisdn = customer.iloc[customer2Id - 1]['msisdn']
            else:
                direction = "---------"
                customerId = instance.iloc[id_product_instance - 1]['customer_id']
                calling_msisdn = customer.iloc[customerId - 1]['msisdn']
                called_msisdn = "---------"

            writer.writerow(
                    [ID, id_product_instance, date, event_type, call_count, sms_count, data_count, roaming, direction, calling_msisdn, called_msisdn])
