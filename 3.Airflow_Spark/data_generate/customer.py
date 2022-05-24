import random
import numpy as np
import pandas as pd
import datetime as dt

data_size = 1000                     # задание объема данных


def gen_first_name(gender):
    male_names = ['Jose', 'Joao', 'Antonio', 'Francisco', 'Carlos', 'Paulo',
                  'Pedro', 'Lucas', 'Luiz', 'Marcos']
    female_names = ['Maria', 'Ana', 'Franciska', 'Antonia', 'Andriana', 'Juliana',
                    'Marcia', 'Fernanda', 'Patricia', 'Aline']
    if gender == 'male':
        return np.random.choice(male_names)
    else:
        return np.random.choice(female_names)


def gen_last_name():
    last_names = ['Silva', 'Santos', 'Sousa', 'Oliveira', 'Pereira', 'Lima', 'Carvalho',
                  'Ferreira', 'Rodrigues', 'Almeida', 'Costa', 'Gomes', 'Martins',
                  'Araujo', 'Melo', 'Barbosa', 'Alves', 'Ribeiro', 'Cardoso']
    return np.random.choice(last_names)


def gen_date_of_birth():
    month = np.random.randint(1, 13)
    if month == 2:
        day = np.random.randint(1, 29)
    elif month in [4, 6, 9, 11]:
        day = np.random.randint(1, 31)
    else:
        day = np.random.randint(1, 32)
    first_part = np.random.normal(loc=20, scale=5, size=100)
    second_part = np.random.normal(loc=36, scale=20, size=1000)
    third_part = np.random.normal(loc=55, scale=20, size=300)
    age_distribution = np.concatenate([first_part, second_part, third_part])
    age_distribution = age_distribution[(age_distribution < 100) & (age_distribution >= 19)]
    year = 2022 - int(np.random.choice(age_distribution))
    return dt.date(year=year, month=month, day=day)


def gen_date_of_start():
    since = dt.date(year=2020, month=1, day=1)
    delta = dt.datetime.now().date() - since
    return since + dt.timedelta(days=np.random.randint(delta.days))


def gen_date_of_term(date_of_start):
    delta = dt.date(year=2030, month=1, day=1) - date_of_start
    term = date_of_start + dt.timedelta(days=np.random.randint(delta.days))
    if term > dt.datetime.now().date():
        return "----------"
    elif term < date_of_start + dt.timedelta(days=31):
        return date_of_start + dt.timedelta(days=31)
    else:
        return term


def gen_promo_agreement(date_of_birth):
    age = 2022 - date_of_birth.year
    if age < 25:
        return np.random.choice(['Yes', 'No'], p=[0.9, 0.1])
    elif age < 30:
        return np.random.choice(['Yes', 'No'], p=[0.8, 0.2])
    elif age < 40:
        return np.random.choice(['Yes', 'No'], p=[0.7, 0.3])
    else:
        return np.random.choice(['Yes', 'No'], p=[0.6, 0.4])


def gen_card():
    chance = np.random.randint(0, 10)
    if chance < 8:
        card = np.random.randint(0, 9, size=16)
        card_str = ''
        for x in card:
            card_str += str(x)
    else:
        card_str = "----------"
    return card_str


def gen_phone():
    phone = np.random.randint(0, 9, size=10)
    phone_str = "+55-"
    phone_str += str(phone[0]) + str(phone[1]) + "-"
    for i in range(8):
        phone_str += str(phone[i])
    return phone_str


def gen_status(date_of_term):
    if date_of_term == "----------":
        return "active"
    else:
        return "inactive"


def gen_category():
    gen = np.random.randint(0, 101)
    if gen < 99:
        return "phyzical"
    else:
        return "business"


def gen_region():
    Regions = ['[-23.5475,-46.6361]', '[-22.9064,-43.1822]', '[-12.9711,-38.5108]', '[-15.77972,-47.92972]',
               '[-3.71722,-38.5431]', '[-19.9208,-43.9378]', '[-3.10194,-60.025]',
               '[-25.4278,-49.2731]', '[-8.05389,-34.8811]', '[-30.0328,-51.2302]',
               '[-1.45583,-48.5044]', '[-16.6786,-49.2539]', '[-23.4628,-46.5333]', '[-22.9056,-47.0608]',
               '[-2.52972,-44.3028]', '[-22.82694,-43.05389]', '[-9.66583,-35.7353]', '[-22.7856,-43.3117]',
               '[-5.795,-35.2094]', '[-5.08917,-42.8019]',
               '[-23.6639,-46.5383]', '[-18.9186,-48.2772]', '[-10.9111,-37.0717]', '[-26.3044,-48.8456]',
               '[-23.9608,-46.3336]']
    Region_weight = [11.9, 6.45, 2.9, 2.85, 2.57, 2.49, 2.02, 1.86, 1.6, 1.47,
                     1.43, 1.41, 1.31, 1.15, 1.06, 1.03, 1.0, 0.88, 0.86, 0.84,
                     0.71, 0.65, 0.62, 0.55, 0.43]

    region = random.choices(Regions, weights = Region_weight)[0]
    return region

def gen_language():
    gen=np.random.randint(1,20)
    if gen>1:
        return "portuguese"
    else:
        return "english"


columns = ['customer_id', 'first_name', 'last_name', 'date_of_birth',        # колонки в таблице
           'gender', 'agree_for_promo', 'autopay_card', 'email', 'msisdn',
           'status', 'customer_category', 'customer_since', 'region',
           'language', 'termination_date']


writer = pd.DataFrame([columns], columns=columns)
writer.to_csv('customer.csv',mode='w', index=False, header=False)            # заполнение таблицы данными
for i in range(data_size):
    ID = i + 1
    gender = np.random.choice(['male', 'female'])
    f_name = gen_first_name(gender)
    l_name = gen_last_name()
    date_of_birth = gen_date_of_birth()
    agree_for_promo = gen_promo_agreement(date_of_birth)
    card = gen_card()
    email = f_name.lower()[0:4] + '.' + l_name.lower()[0:5] + str(date_of_birth.year % 100) + '@gmail.com'
    msisdn = gen_phone()
    category = gen_category()
    since = gen_date_of_start()
    region = gen_region()
    language = gen_language()
    termination_date = gen_date_of_term(since)
    status = gen_status(termination_date)
    writer = pd.DataFrame([[ID, f_name, l_name, date_of_birth, gender, agree_for_promo, card, email, msisdn, status, category, since, region, language, termination_date]], columns=columns)
    writer.to_csv('customer.csv', mode='a', index=False, header=False)

