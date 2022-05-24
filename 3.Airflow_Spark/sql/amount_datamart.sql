select date_part('year', p.date::timestamp) as year, date_part('month', p.date::timestamp) as month, sum(p.amount) as amount, current_timestamp as execution_timestamp
                    from raw.payments as p
                    group by date_part('year', p.date::timestamp), date_part('month', p.date::timestamp)
                    order by date_part('year', p.date::timestamp), date_part('month', p.date::timestamp) asc