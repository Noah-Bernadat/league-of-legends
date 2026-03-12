select *
from {{ source('raw', 'data') }}