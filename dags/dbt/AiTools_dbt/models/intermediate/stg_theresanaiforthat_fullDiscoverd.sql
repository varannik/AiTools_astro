
{{ config(
  materialized = 'table'
)}}

with

frn_ as (select * from {{source('theresanaiforthat', 'ref_theresanaiforthat_allAis')}}),
int_ as (select * from {{source('theresanaiforthat', 'ref_theresanaiforthat_features')}}),
trg_ as (select * from {{source('theresanaiforthat', 'ref_theresanaiforthat_target_features')}}),


---------------- front
mid_aa as(
select *
,row_number() over (partition by url_internal order by insert_date desc) rn
from frn_
),
mid_ab as(
select
task
,"name"
,url_internal
from mid_aa where rn=1
),

-------------- mid
mid_ba as (
select
rate
,count_rate
,count_save
,count_comments
,tags
,price
,url_screen_shot
,url_ai
,url_internal
,row_number() over (partition by url_internal order by insert_date desc) rn
from int_
),
mid_bb as (
select * from mid_ba where rn = 1
),

-------------- mid final
mid_ as (
select *
from mid_ab
right join mid_bb
using (url_internal)
),

---------------- target
end_aa as (
select *
,row_number() over (partition by url_ai order by insert_date desc) rn
from trg_
),
end_ab as (
select
url_ai
,url_stb
,url_fav
,url_log
from end_aa
where rn = 1
),

------------- All together
end_ as(
select
task
,"name"
,rate
,count_rate
,count_save
,count_comments
,tags
,price
,url_screen_shot
,url_stb
,url_fav
,url_log

from mid_
right join end_ab
using(url_ai)
)

select
task 					    as task_tht
,"name" 				  as name_tht
,rate					    as rate_tht
,count_rate				as count_rate_tht
,count_save				as count_save_tht
,count_comments		as count_comment_tht
,tags					    as tags_tht
,price					  as price_tht
,url_screen_shot  as url_internal_tht
,url_stb				  as url_stb_tht
,url_fav				  as url_fav_tht
,url_log				  as url_log_tht
from end_
