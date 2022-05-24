select distinct c.customer_id, (select date_part('year',age(c.date_of_birth::date))) as age, c.gender, c.autopay_card, c.email, c.msisdn, c.region, c.language,
                        (select sum(call_count) from raw.costed_event where c.msisdn = raw.costed_event.calling_msisdn and raw.costed_event.date::date >= date_trunc('day',current_timestamp - interval '1 month') and raw.costed_event.date::date < date_trunc('day',current_timestamp)) AS sumCall,
                        (select sum(sms_count) from raw.costed_event where c.msisdn = raw.costed_event.calling_msisdn and raw.costed_event.date::date >= date_trunc('day',current_timestamp - interval '1 month') and raw.costed_event.date::date < date_trunc('day',current_timestamp)) AS sumSms,
                        (select sum(data_count) from raw.costed_event where c.msisdn = raw.costed_event.calling_msisdn and raw.costed_event.date::date >= date_trunc('day',current_timestamp - interval '1 month') and raw.costed_event.date::date < date_trunc('day',current_timestamp)) AS sumData,
                        i.activation_date, p2.allowance_voice, p2.allowance_sms, p2.allowance_data,
                        (select round(sum(call_count)/3,0) from raw.costed_event where c.msisdn = raw.costed_event.calling_msisdn and raw.costed_event.date::date >= date_trunc('day',current_timestamp - interval '3 month') and raw.costed_event.date::date < date_trunc('day',current_timestamp)) AS medium_call_count,
                        (select round(sum(sms_count)/3,0) from raw.costed_event where c.msisdn = raw.costed_event.calling_msisdn and raw.costed_event.date::date >= date_trunc('day',current_timestamp - interval '3 month') and raw.costed_event.date::date < date_trunc('day',current_timestamp)) AS medium_sms_count,
                        (select round(sum(data_count)/3,0) from raw.costed_event where c.msisdn = raw.costed_event.calling_msisdn and raw.costed_event.date::date >= date_trunc('day',current_timestamp - interval '3 month') and raw.costed_event.date::date < date_trunc('day',current_timestamp)) AS medium_Gb_count,
                        current_timestamp as execution_timestamp
                    from raw.customer as c
                    join raw.payments as p on c.customer_id=p.customer_id
                    join raw.instance as i on c.customer_id=i.customer_id
                    join raw.product as p2 on i.product_id=p2.product_id
                    join raw.costed_event as c2 on i.product_instance_id=c2.product_instance_id
                    where c.agree_for_promo = 'Yes' and c.status = 'active' and customer_category = 'phyzical' and i.status = 'active'
                    group by c.customer_id, age, c.gender, c.autopay_card, c.email, c.msisdn, c.region, c.language, sumCall, sumSms, sumData, i.activation_date, p2.allowance_voice, p2.allowance_sms, p2.allowance_data
                    order by c.customer_id