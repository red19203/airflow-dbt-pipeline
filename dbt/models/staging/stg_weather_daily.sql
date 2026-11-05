select
    city,
    date,
    temperature_max,
    temperature_min,
    round(((temperature_max + temperature_min) / 2)::numeric, 1) as temperature_avg,
    precipitation_sum,
    precipitation_sum > 1 as is_rainy,
    loaded_at
from {{ source('raw', 'weather_daily') }}
where temperature_max is not null
  and temperature_min is not null
