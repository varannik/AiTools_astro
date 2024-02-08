{{ config(
  materialized = 'table'
)}}



with

int_ as (select * from {{source ('aitoolhunt', 'ref_aitoolhunt_features')}}),
trg_ as (select * from {{source ('aitoolhunt', 'ref_aitoolhunt_target_features')}}),

mid_a as (

select
cat
,pricing
,semrush_rank
,tech_used
,url_ai
,"name"
,tags
,url_screen_shot
,"year"
,row_number() over (partition by url_internal order by insert_date desc) rn
from int_
),

end_a as (
select *
,row_number() over (partition by url_ai order by insert_date desc) rn
from trg_
),

mid_ as ( select * from mid_a where rn = 1),
end_ as ( select * from end_a where rn = 1),

out_ as (
select *
from mid_
right join end_
using(url_ai)
)

select
"name"				    as name_ath
,cat				      as cat_ath
,pricing			    as price_ath
,semrush_rank		  as semrush_rank_ath
,tech_used			  as tech_used_ath
,tags				      as tags_ath
,url_screen_shot	as url_screen_shot_ath
,"year"				    as year_ath
,url_stb			    as url_stb_ath
,url_fav			    as url_fav_ath
,url_log			    as url_log_ath
from out_
