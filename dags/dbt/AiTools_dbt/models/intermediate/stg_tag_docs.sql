{{ config(
  materialized = 'table'
)}}


with

 ath_f as (select * from {{source('aitoolhunt', 'ref_aitoolhunt_features')}})
,ath_t as (select * from {{source('aitoolhunt', 'ref_aitoolhunt_target_features')}})

,tht_f as (select * from {{source('theresanaiforthat', 'ref_theresanaiforthat_features')}})
,tht_n as (select * from {{source('theresanaiforthat', 'ref_theresanaiforthat_allAis')}})
,tht_t as (select * from {{source('theresanaiforthat', 'ref_theresanaiforthat_target_features')}})
,tht_a as (select * from {{source('theresanaiforthat', 'ref_theresanaiforthat_attributes')}})
,tht_d as (select * from {{source('theresanaiforthat', 'ref_theresanaiforthat_description')}})


,ath_fea_last  as (select *,row_number() over(partition by url_ai order by insert_date desc) rn  from ath_f)
,ath_trg_last as (select *,row_number() over(partition by url_ai order by insert_date desc) rn 	from ath_t)


,tht_fea_last as (select *,row_number() over(partition by url_internal order by insert_date desc) rn from tht_f)
,tht_nam_last as (select *,row_number() over(partition by url_internal order by insert_date desc) rn from tht_n)
,tht_trg_last as (select *,row_number() over(partition by url_ai order by insert_date desc) rn from tht_t)
,tht_att_last as (select url_internal,description as description_s ,row_number() over(partition by url_internal,description  order by insert_date desc) rn  from tht_a)
,tht_des_last as (select url_internal,description  as description_l ,row_number() over(partition by url_internal,description  order by insert_date desc) rn  from tht_d)


,ath_fea 	as (select * from ath_fea_last where rn = 1)
,ath_trg	as (select * from ath_trg_last where rn = 1)



,tht_fea	as (select * from tht_fea_last where rn = 1)
,tht_att	as (select * from tht_att_last where rn = 1)
,tht_des	as (select * from tht_des_last where rn = 1)
,tht_trg	as (select * from tht_trg_last where rn = 1)



,tht_fea_j as (select url_stb, usecase 	 	as doc 	from tht_fea join tht_trg using(url_ai))
--,tht_att_j as (select url_stb, description_s as doc 	from (select * from tht_att join tht_fea using(url_internal)) join tht_trg using(url_ai))
,tht_des_j as (select url_stb, description_l as doc 	from (select * from tht_des join tht_fea using(url_internal)) join tht_trg using(url_ai))
,ath_use_j as (select url_stb, use_cases 	as doc 	from ath_fea join ath_trg using(url_ai))
,ath_des_j as (select url_stb, description  as doc 	from ath_fea join ath_trg using(url_ai))
,ath_fea_j as (select url_stb, features  	as doc 	from ath_fea join ath_trg using(url_ai))


,uni_ as (

select * from tht_fea_j
union
select * from tht_des_j
union
select * from ath_use_j
union
select * from ath_des_j
union
select * from ath_des_j
order by url_stb

)

select * from uni_


