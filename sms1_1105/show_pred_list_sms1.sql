
--以下表需要更新：
--tmp.djjf_richuser_pred_list_yyall_send
--app.jfrz_11_5
--tmp.djjf_richuser_jr7active_10w_idx

--简表
--2845712
drop table if exists tmp.djjf_richuser_pred_list_short;
create table tmp.djjf_richuser_pred_list_short
as
select 
* 
--from tmp.djjf_richuser_pred_list_yyall_send
FROM tmp.djjf_richuser_pred_list_jdmall_visit
where prob1>0.4
;

drop table if exists tmp.djjf_richuser_pred_list_sort;
create table tmp.djjf_richuser_pred_list_sort
as
select
row_number() over (order by t.prob1 desc) as rn,
*
from tmp.djjf_richuser_pred_list_short t
;

drop table if exists tmp.djjf_richuser_pred_list_rand;
create table tmp.djjf_richuser_pred_list_rand
as
select

	case when rn>0 and rn<=2845712*0.2
		 then 1
		 when rn>2845712*0.2 and rn<=2845712*0.4
		 then 2
		 when rn>2845712*0.4 and rn<=2845712*0.6
		 then 3
		 when rn>2845712*0.6 and rn<=2845712*0.8
		 then 4
		 when rn>2845712*0.8 and rn<=2845712
		 then 5
		 else -1
	end as group_no,
	rand()*2845712 as rand_idx,
	case when dz.jdpin is null
		then 0
		else 1
	end as is_dz,
	*
from tmp.djjf_richuser_pred_list_sort t
left join tmp.zqp_jfdz_distinct_jdpin dz
on t.acct = dz.jdpin
;

--<check>统计各组到站情况
--173679	
--72859	35729	27012	20798	17281
drop table if exists tmp.djjf_richuser_pred_list_group_stat;
create table tmp.djjf_richuser_pred_list_group_stat
as
select
sum(case when is_dz=1
	then 1
	else 0
end) as dznum,

sum(case when is_dz=1 and group_no=1
	then 1
	else 0
end) as dznum_g1,

sum(case when is_dz=1 and group_no=2
	then 1
	else 0
end) as dznum_g2,

sum(case when is_dz=1 and group_no=3
	then 1
	else 0
end) as dznum_g3,

sum(case when is_dz=1 and group_no=4
	then 1
	else 0
end) as dznum_g4,

sum(case when is_dz=1 and group_no=5
	then 1
	else 0
end) as dznum_g5

from tmp.djjf_richuser_pred_list_rand
;

drop table if exists tmp.djjf_richuser_pred_list_yymatch_stat0;
create table tmp.djjf_richuser_pred_list_yymatch_stat0
as
select 
	count(distinct case when substr(t.yy_time,1,10) = '2018-10-27'
		     then lower(trim(t.jdpin))
			 else null
		end) as cnt27,
	count(distinct case when substr(t.yy_time,1,10) = '2018-10-27' and st.rn<=300000
		     then lower(trim(t.jdpin))
			 else null
		end) as yycnt27,
	count(distinct case when substr(t.yy_time,1,10) = '2018-10-28'
		     then lower(trim(t.jdpin))
			 else null
		end) as cnt28,
	count(distinct case when substr(t.yy_time,1,10) = '2018-10-28' and st.rn<=300000
		     then lower(trim(t.jdpin))
			 else null
		end) as yycnt28
from app.djjf_yy_all t
left join tmp.djjf_richuser_pred_list_sort st
on lower(trim(t.jdpin)) = lower(trim(st.acct))
;

drop table if exists tmp.djjf_richuser_pred_list_features;
create table tmp.djjf_richuser_pred_list_features
as
select
t.group_no,
t.rand_idx,
t.rn,
t.prediction,
t.prob0,
t.prob1,
t.is_dz,
rd.*
from tmp.djjf_richuser_pred_list_rand t
inner join tmp.djjf_sms2_pred_features rd
on t.acct = rd.acct
;

--禁止发送名单，北京禁止发送,7内不活跃的排除
drop table if exists tmp.djjf_richuser_pred_list_ban;
create table tmp.djjf_richuser_pred_list_ban
as
select 
	lower(trim(user_log_acct)) as acct,
	datediff('2018-10-26', max(dt)) as last_visit_days,
	sum(case when user_site_province_name='北京' or user_site_city_name='北京'
			then 1
			else 0
		end) as bj_visit_cnt
from adm.adm_s14_ol_user_di
where dt<=date_add('2018-10-26', -1) and dt>=date_add('2018-10-26', -30) and length(lower(trim(user_log_acct)))>0
group by lower(trim(user_log_acct))
;

--营销名单准备，此处排除已预约名单
--排除10.26之前的预约用户
drop table if exists tmp.djjf_richuser_pred_list_prepare;
create table tmp.djjf_richuser_pred_list_prepare
as
select
	row_number() over (order by t.prob1 desc) as idx,
	t.*
from tmp.djjf_richuser_pred_list_features t
left join 
	(select 
		distinct acct
	from tmp.djjf_sms2_train_yy_list
	where ddate <'2018-10-26') bfyy
	on lower(trim(t.acct)) = lower(trim(bfyy.acct))
left join tmp.djjf_richuser_pred_list_ban ban
	on lower(trim(t.acct)) = lower(trim(ban.acct))
where t.rn<=1000000 and (bfyy.acct is null) and ban.last_visit_days<=7 and ban.bj_visit_cnt<=0
;

--实验组,30w
drop table if exists tmp.djjf_richuser_pred_list_yymatch_stat1;
create table tmp.djjf_richuser_pred_list_yymatch_stat1
as
select 
	count(distinct case when substr(t.yy_time,1,10) = '2018-10-27'
		     then lower(trim(t.jdpin))
			 else null
		end) as cnt27,
	count(distinct case when substr(t.yy_time,1,10) = '2018-10-27' and st.idx<=300000
		     then lower(trim(t.jdpin))
			 else null
		end) as yycnt27,
	count(distinct case when substr(t.yy_time,1,10) = '2018-10-28'
		     then lower(trim(t.jdpin))
			 else null
		end) as cnt28,
	count(distinct case when substr(t.yy_time,1,10) = '2018-10-28' and st.idx<=300000
		     then lower(trim(t.jdpin))
			 else null
		end) as yycnt28
from app.djjf_yy_all t
left join tmp.djjf_richuser_pred_list_prepare st
on lower(trim(t.jdpin)) = lower(trim(st.acct))
;

drop table if exists tmp.djjf_richuser_pred_list_prepare_sms1model_1121;
create table tmp.djjf_richuser_pred_list_prepare_sms1model_1121
as
select
*
from tmp.djjf_richuser_pred_list_prepare
;
