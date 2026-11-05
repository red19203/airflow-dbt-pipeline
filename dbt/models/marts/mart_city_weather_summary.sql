select
    city,
    count(*) as nb_days,
    min(date) as first_date,
    max(date) as last_date,
    round(avg(temperature_avg)::numeric, 1) as avg_temperature,
    max(temperature_max) as max_temperature,
    min(temperature_min) as min_temperature,
    round(sum(precipitation_sum)::numeric, 1) as total_precipitation,
    count(*) filter (where is_rainy) as rainy_days
from {{ ref('stg_weather_daily') }}
group by city
