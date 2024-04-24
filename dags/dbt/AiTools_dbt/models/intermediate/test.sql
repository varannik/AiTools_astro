{{ config(
  materialized = 'table'
)}}


with 

tht_ as (select * from {{source('theresanaiforthat', 'ref_theresanaiforthat_features')}})


select * , {{ get_platform_from_url('url_ai') }} as me
from tht_
