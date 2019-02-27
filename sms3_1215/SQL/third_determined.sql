--当前时间为 2018-12-14
--7天活跃且在模型人数 672846 需改


--7天活跃用户表
DROP TABLE IF EXISTS dev_tmp.cjh_7active_1026;
CREATE TABLE dev_tmp.cjh_7active_1026 AS
SELECT 
	lower(trim(pin)) as pin
FROM dwd.dwd_brs_jr_jrapp_userstat_i_d
WHERE dt>=date_add('2018-12-14',-7)
  AND dt<=date_add('2018-12-14',-1)
  and length(lower(trim(pin)))>0
group by lower(trim(pin))
;

--一年内基金单用户
DROP TABLE IF EXISTS dev_tmp.cjh_jjd_user;
CREATE TABLE dev_tmp.cjh_jjd_user AS
select 
	lower(trim(jd_pin)) as pin
from dwd.dwd_basic_fin_fund_trade_s_d
where dt=date_add('2018-12-14',-1)
  AND tx_type='purch'
  and length(lower(trim(jd_pin)))>0
  and to_date(tx_succ_time)>=date_add('2018-12-14',-365)
  and to_date(tx_succ_time)<=date_add('2018-12-14',-1)
group by lower(trim(jd_pin))
;

--一年内基金预约用户表
DROP TABLE IF EXISTS dev_tmp.cjh_jjyy_user;
CREATE TABLE dev_tmp.cjh_jjyy_user AS
SELECT lower(trim(jd_pin)) as pin
FROM dwd.dwd_fin_fund_fixed_purchase_task_s_d
WHERE dt>=date_add('2018-12-14',-365)
  AND dt<=date_add('2018-12-14',-1)
  and length(lower(trim(jd_pin)))>0
  and status=1 --1代表成功
group by  lower(trim(jd_pin))
;

--30天点击股票财迷埋点
DROP TABLE IF EXISTS dev_tmp.cjh_30click_stock_user;
CREATE TABLE dev_tmp.cjh_30click_stock_user AS
SELECT lower(trim(pin)) as pin
FROM dwb.dwb_brs_jr_web_click_fin_stock_log_clstag_i_d
WHERE dt>=date_add('2018-12-14',-30)
  AND dt<=date_add('2018-12-14',-1)
  and length(lower(trim(pin)))>0
group by  lower(trim(pin))
;

--30天浏览东家财富页面用户
DROP TABLE IF EXISTS dev_tmp.cjh_djjf_30view_user;
CREATE TABLE dev_tmp.cjh_djjf_30view_user AS
SELECT
	lower(trim(pin)) as pin
FROM dwd.dwd_brs_jr_jdjrflow_web_click_log_i_d
WHERE dt>=date_add('2018-12-14', -30)
  AND dt<=date_add('2018-12-14', -1)
  AND REGEXP(requesturl, 'rich.jd.com')
  AND length(lower(trim(pin)))>0
group by lower(trim(pin))
;

--7活+30天大理财埋点点击用户名单（需与100W）
DROP TABLE IF EXISTS dev_tmp.cjh_7click_dlc_1026;
CREATE TABLE dev_tmp.cjh_7click_dlc_1026 AS
SELECT
	lower(trim(t.pin)) as pin
FROM dwb.dwb_brs_jr_web_click_fin_log_clstag_i_d t
inner join dev_tmp.cjh_7active_1026 ac
on lower(trim(t.pin))=lower(trim(ac.pin))
WHERE dt>=date_add('2018-12-14',-30)
  AND dt<=date_add('2018-12-14',-1)
  and length(lower(trim(t.pin)))>0
group by lower(trim(t.pin))
;


--7天保障险用户总额名单
drop table if exists dev_tmp.cjh_bxpd_7day;
create table dev_tmp.cjh_bxpd_7day as
select
	lower(trim(pin)) as pin,
    sum(totalprice) as 7dayamt
from dwd.dwd_actv_insu_bxpd_order_s_d
where dt>=date_add('2018-12-14',-7) and dt<=date_add('2018-12-14',-1)
	and length(lower(trim(pin)))>0
group by lower(trim(pin))
;

--7天保障险大于40名单（需与100W）
drop table if exists dev_tmp.cjh_bxpd_7day_user;
create table dev_tmp.cjh_bxpd_7day_user as
select
	lower(trim(t.pin)) as pin
from dev_tmp.cjh_bxpd_7day t
where t.7dayamt>40
	and length(lower(trim(t.pin)))>0
group by lower(trim(t.pin))
;

--7活+900天保险理财交易单用户表（需与350W匹配）
drop table if exists dev_tmp.cjh_bxlc_user;
create table dev_tmp.cjh_bxlc_user as
select
	lower(trim(t.pin)) as pin
from dwd.dwd_basic_fin_insu_trade_s_d t
inner join dev_tmp.cjh_7active_1026 ac
on lower(trim(t.pin))=lower(trim(ac.pin))
where dt=date_add('2018-12-14',-1) and tx_type='purch'
	and length(lower(trim(t.pin)))>0
	and to_date(ordertime)>=date_add('2018-12-14',-900)
    and to_date(ordertime)<=date_add('2018-12-14',-1)
group by lower(trim(t.pin))
;

--券商理财交易单（需与350W）
drop table if exists dev_tmp.cjh_qslc_user;
create table dev_tmp.cjh_qslc_user as
select
	lower(trim(user_pin)) as pin
from dwd.dwd_basic_fin_qslc_trade_s_d t
where dt=date_add('2018-12-14',-1) and tx_type='purch'
	and length(lower(trim(user_pin)))>0
group by lower(trim(user_pin))
;

--构建760W模型
/*
DROP TABLE IF EXISTS dev_tmp.cjh_model_pred_list_1122_1000W;
create table dev_tmp.cjh_model_pred_list_1122_1000W(
	idx bigint,
    acct String,
	mall_last_visit int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';*/

--构建top100W
DROP TABLE IF EXISTS dev_tmp.cjh_model_pred_list_1122_1000W_top100W;
CREATE TABLE dev_tmp.cjh_model_pred_list_1122_1000W_top100W AS
select * from dev_tmp.cjh_model_pred_list_1122_1000W
where idx<=1000000
;

--构建top350W
DROP TABLE IF EXISTS dev_tmp.cjh_model_pred_list_1122_1000W_top350W;
CREATE TABLE dev_tmp.cjh_model_pred_list_1122_1000W_top350W AS
select * from dev_tmp.cjh_model_pred_list_1122_1000W
where idx<=3500000
;

--760W模型+老标签下用户名单(基金单购入，基金单预约，三十天点击股票埋点，30天东家金服浏览)
DROP TABLE IF EXISTS dev_tmp.cjh_750W_label_user;
CREATE TABLE dev_tmp.cjh_750W_label_user AS
select
	lower(trim(md.acct)) as acct
from dev_tmp.cjh_model_pred_list_1122_1000W md
inner join
(select lower(trim(all_u.pin)) as pin from
	(select pin from dev_tmp.cjh_jjd_user
	 union all
	 select pin from dev_tmp.cjh_jjyy_user
	 union all
	 select pin from dev_tmp.cjh_30click_stock_user
	 union all
	 select pin from dev_tmp.cjh_djjf_30view_user) all_u
) all_u
on lower(trim(md.acct))=lower(trim(all_u.pin))
group by lower(trim(md.acct))
;

--100W模型+标签下用户名单(30大理财点击+保障险总额>40)
DROP TABLE IF EXISTS dev_tmp.cjh_100W_label_user;
CREATE TABLE dev_tmp.cjh_100W_label_user AS
select
	lower(trim(md.acct)) as acct
from dev_tmp.cjh_model_pred_list_1122_1000W_top100W md
inner join
(select lower(trim(all_u.pin)) as pin from
	(select pin from dev_tmp.cjh_7click_dlc_1026
	 union all
	 select pin from dev_tmp.cjh_bxpd_7day_user
	 ) all_u
) all_u
on lower(trim(md.acct))=lower(trim(all_u.pin))
group by lower(trim(md.acct))
;

--350W模型+标签下用户名单(券商理财交易单+保险理财交易单)
DROP TABLE IF EXISTS dev_tmp.cjh_350W_label_user;
CREATE TABLE dev_tmp.cjh_350W_label_user AS
select
	lower(trim(md.acct)) as acct
from dev_tmp.cjh_model_pred_list_1122_1000W_top350W md
inner join
(select lower(trim(all_u.pin)) as pin from
	(select pin from dev_tmp.cjh_bxlc_user
	 union all
	 select pin from dev_tmp.cjh_qslc_user
	 ) all_u
) all_u
on lower(trim(md.acct))=lower(trim(all_u.pin))
group by lower(trim(md.acct))
;

--需检查人数后执行
--七天活跃模型用户
DROP TABLE IF EXISTS dev_tmp.cjh_7active_md_user;
CREATE TABLE dev_tmp.cjh_7active_md_user AS
SELECT 
	lower(trim(t.pin)) as pin,
	rand()*672846 AS sample_idx
FROM dev_tmp.cjh_7active_1026 t
inner join dev_tmp.cjh_model_pred_list_1122_1000W_top100W md
on lower(trim(md.acct))=lower(trim(t.pin))
WHERE length(lower(trim(t.pin)))>0
group by lower(trim(t.pin))
;

--七天活跃模型用户随机取2W
DROP TABLE IF EXISTS dev_tmp.cjh_7active_ran_pick;
CREATE TABLE dev_tmp.cjh_7active_ran_pick AS
SELECT 
	lower(trim(t.pin)) as pin
FROM dev_tmp.cjh_7active_md_user t
WHERE length(lower(trim(t.pin)))>0 and t.sample_idx<20000
group by lower(trim(t.pin))
;

--三规格模型汇总
DROP TABLE IF EXISTS dev_tmp.cjh_all3model_user;
CREATE TABLE dev_tmp.cjh_all3model_user AS
select
	lower(trim(md.acct)) as acct
from 
(select lower(trim(all_u.acct)) as acct from
	(select acct from dev_tmp.cjh_100W_label_user
	 union all
	 select acct from dev_tmp.cjh_350W_label_user
	 union all
	 select acct from dev_tmp.cjh_750W_label_user
	 union all
	 select pin as acct from dev_tmp.cjh_7active_ran_pick
	) all_u
) md
group by lower(trim(md.acct))
;
--将总表分块传下
DROP TABLE IF EXISTS dev_tmp.cjh_all3model_user_index;
CREATE TABLE dev_tmp.cjh_all3model_user_index AS
select
	acct,
	rand()*20000 AS sample_idx
from dev_tmp.cjh_all3model_user
;

DROP TABLE IF EXISTS dev_tmp.cjh_all3model_user_p1;
CREATE TABLE dev_tmp.cjh_all3model_user_p1 AS
select
	acct
from dev_tmp.cjh_all3model_user_index
where sample_idx <10000
;

DROP TABLE IF EXISTS dev_tmp.cjh_all3model_user_p2;
CREATE TABLE dev_tmp.cjh_all3model_user_p2 AS
select
	acct
from dev_tmp.cjh_all3model_user_index
where sample_idx >=10000
;

--从猛犸系统拉取表格准备
DROP TABLE IF EXISTS dev_tmp.cjh_all3model_user_p1_trans;
CREATE TABLE dev_tmp.cjh_all3model_user_p1_trans AS
SELECT trans.flag,
       concat_ws('&,', collect_set(trans.acct)) AS pin
FROM
	(SELECT t.acct,
          1 AS flag
     FROM dev_tmp.cjh_all3model_user_p1 t
     GROUP BY t.acct
	) trans
GROUP BY trans.flag
;

DROP TABLE IF EXISTS dev_tmp.cjh_all3model_user_p2_trans;
CREATE TABLE dev_tmp.cjh_all3model_user_p2_trans AS
SELECT trans.flag,
       concat_ws('&,', collect_set(trans.acct)) AS pin
FROM
	(SELECT t.acct,
          1 AS flag
     FROM dev_tmp.cjh_all3model_user_p2 t
     GROUP BY t.acct
	) trans
GROUP BY trans.flag
;

-------此处开始统计信息sql-----------
--7活+30天大理财埋点点击用户统计
DROP TABLE IF EXISTS dev_tmp.cjh_7click_dlc_1026_stat;
CREATE TABLE dev_tmp.cjh_7click_dlc_1026_stat AS
select
	count(t.pin) as cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as yy28_cnt
from dev_tmp.cjh_7click_dlc_1026 t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
inner join dev_tmp.cjh_model_pred_list_1122_1000W_top100W md
on lower(trim(t.pin))=lower(trim(md.acct))
;


--保障险统计
drop table if exists dev_tmp.cjh_bxpd_7day_stat;
create table dev_tmp.cjh_bxpd_7day_stat as
select
	count(t.pin) as cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as yy28_cnt
from dev_tmp.cjh_bxpd_7day_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
inner join dev_tmp.cjh_model_pred_list_1122_1000W_top100W md
on lower(trim(t.pin))=lower(trim(md.acct))
;


--7活+900天保险理财交易单统计
drop table if exists dev_tmp.cjh_bxlc_user_stat;
create table dev_tmp.cjh_bxlc_user_stat as
select
	count(t.pin) as cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as yy28_cnt
from dev_tmp.cjh_bxlc_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
inner join dev_tmp.cjh_model_pred_list_1122_1000W_top350W md
on lower(trim(t.pin))=lower(trim(md.acct))
;


--券商理财交易单统计
drop table if exists dev_tmp.cjh_qslc_user_stat;
create table dev_tmp.cjh_qslc_user_stat as
select
	count(t.pin) as cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as yy28_cnt
from dev_tmp.cjh_qslc_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
inner join dev_tmp.cjh_model_pred_list_1122_1000W_top350W md
on lower(trim(t.pin))=lower(trim(md.acct))
;

--七天活跃加模型效果统计
DROP TABLE IF EXISTS dev_tmp.cjh_7active_user_stat;
CREATE TABLE dev_tmp.cjh_7active_user_stat AS
select
	count(t.pin) as cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as yy28_cnt
from dev_tmp.cjh_7active_md_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
;
--750W模型效果统计
DROP TABLE IF EXISTS dev_tmp.cjh_750W_label_user_stat;
CREATE TABLE dev_tmp.cjh_750W_label_user_stat AS
select
	count(t.acct) as cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as yy28_cnt
from dev_tmp.cjh_750W_label_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.acct))=lower(trim(yy.acct))
;

--350W模型效果统计
DROP TABLE IF EXISTS dev_tmp.cjh_350W_label_user_stat;
CREATE TABLE dev_tmp.cjh_350W_label_user_stat AS
select
	count(t.acct) as cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as yy28_cnt
from dev_tmp.cjh_350W_label_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.acct))=lower(trim(yy.acct))
;

--100W模型效果统计
DROP TABLE IF EXISTS dev_tmp.cjh_100W_label_user_stat;
CREATE TABLE dev_tmp.cjh_100W_label_user_stat AS
select
	count(t.acct) as cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as yy28_cnt
from dev_tmp.cjh_100W_label_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.acct))=lower(trim(yy.acct))
;

--最终模型效果统计
drop table if exists dev_tmp.cjh_finalmodel_stat;
create table dev_tmp.cjh_finalmodel_stat as
select
	count(t.acct) as cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as yy28_cnt
from dev_tmp.cjh_all3model_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.acct))=lower(trim(yy.acct))
;

