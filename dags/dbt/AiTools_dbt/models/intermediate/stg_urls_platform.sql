
{{ config(
  materialized = 'table'
)}}


with 

tht_ as (select * from {{source('theresanaiforthat', 'ref_theresanaiforthat_features')}}),
ath_ as (select * from {{source('aitoolhunt', 'ref_aitoolhunt_features')}}),

uni_ as (

select url_ai
from  tht_
union 
select url_ai
from  ath_

), 

uni_unique as ( 

SELECT  *
FROM    (SELECT url_ai,
                ROW_NUMBER() OVER (PARTITION BY url_ai) AS RowNumber
         FROM   uni_) AS a
WHERE   a.RowNumber = 1

)

select * from uni_unique