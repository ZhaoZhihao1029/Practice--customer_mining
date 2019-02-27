/*
--预约总表
DROP TABLE IF EXISTS dev_tmp.djjf_yy_all;
create table dev_tmp.djjf_yy_all(
	acct String,
    channel String,
	dtime String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

--预测总标
DROP TABLE IF EXISTS dev_tmp.cjh_model_pred_list_1122_350W;
create table dev_tmp.cjh_model_pred_list_1122_350W(
	idx bigint,
    acct String,
	mall_last_visit int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';
*/

/*
--1106发送名单建表语句
DROP TABLE IF EXISTS dev_tmp.cjh_send_1106;
create table dev_tmp.cjh_send_1106(
	pin String,
	ddate String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';*/

/*
--商城对照组建表
DROP TABLE IF EXISTS dev_tmp.mall_contrast_1122;
create table dev_tmp.mall_contrast_1122(
	acct String,
	idx bigint)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';*/

--27日，28日用户预约表（去重,两天内用户不去重）
DROP TABLE IF EXISTS dev_tmp.cjh_djjf_yy2728;
CREATE TABLE dev_tmp.cjh_djjf_yy2728 AS
SELECT
	lower(trim(acct)) as acct,
	to_date(dtime) as yytime
FROM dev_tmp.djjf_yy_all
WHERE length(lower(trim(acct)))>0 and to_date(dtime)>='2018-10-27' and to_date(dtime)<'2018-10-29'
group by lower(trim(acct)),to_date(dtime)
;

--7天活跃用户表
DROP TABLE IF EXISTS dev_tmp.cjh_7active_1026;
CREATE TABLE dev_tmp.cjh_7active_1026 AS
SELECT 
	lower(trim(pin)) as pin
FROM dwd.dwd_brs_jr_jrapp_userstat_i_d
WHERE dt>=date_add('2018-11-28',-7)
  AND dt<=date_add('2018-11-28',-1)
  and length(lower(trim(pin)))>0
group by lower(trim(pin))
;

--可投资产大于50W用户表
DROP TABLE IF EXISTS dev_tmp.cjh_property50W_user;
CREATE TABLE dev_tmp.cjh_property50W_user AS
select
	lower(trim(user_id)) as user_id
from dmt.dmt_upf_app_cs_touzi_i_d 
where dt>=date_add('2018-11-14',-7) and dt<=date_add('2018-11-14',-1)
	  and property_estimate>500000 and length(lower(trim(user_id)))>0
group by lower(trim(user_id))
;

--一年内基金单用户
DROP TABLE IF EXISTS dev_tmp.cjh_jjd_user;
CREATE TABLE dev_tmp.cjh_jjd_user AS
select 
	lower(trim(jd_pin)) as pin
from dwd.dwd_basic_fin_fund_trade_s_d
where dt=date_add('2018-11-28',-1)
  AND tx_type='purch'
  and length(lower(trim(jd_pin)))>0
  and to_date(tx_succ_time)>=date_add('2018-11-28',-365)
  and to_date(tx_succ_time)<=date_add('2018-11-28',-1)
group by lower(trim(jd_pin))
;

--一年内基金预约用户表
DROP TABLE IF EXISTS dev_tmp.cjh_jjyy_user;
CREATE TABLE dev_tmp.cjh_jjyy_user AS
SELECT lower(trim(jd_pin)) as pin
FROM dwd.dwd_fin_fund_fixed_purchase_task_s_d
WHERE dt>=date_add('2018-11-28',-365)
  AND dt<=date_add('2018-11-28',-1)
  and length(lower(trim(jd_pin)))>0
  and status=1 --1代表成功
group by  lower(trim(jd_pin))
;

--30天点击股票财迷埋点
DROP TABLE IF EXISTS dev_tmp.cjh_30click_stock_user;
CREATE TABLE dev_tmp.cjh_30click_stock_user AS
SELECT lower(trim(pin)) as pin
FROM dwb.dwb_brs_jr_web_click_fin_stock_log_clstag_i_d
WHERE dt>=date_add('2018-11-28',-30)
  AND dt<=date_add('2018-11-28',-1)
  and length(lower(trim(pin)))>0
group by  lower(trim(pin))
;

--30天浏览东家财富页面用户
DROP TABLE IF EXISTS dev_tmp.cjh_djjf_30view_user;
CREATE TABLE dev_tmp.cjh_djjf_30view_user AS
SELECT
	lower(trim(pin)) as pin
FROM dwd.dwd_brs_jr_jdjrflow_web_click_log_i_d
WHERE dt>=date_add('2018-11-28', -30)
  AND dt<=date_add('2018-11-28', -1)
  AND REGEXP(requesturl, 'rich.jd.com')
  AND length(lower(trim(pin)))>0
group by lower(trim(pin))
;

--7天大理财埋点点击用户名单
DROP TABLE IF EXISTS dev_tmp.cjh_7click_dlc_1026;
CREATE TABLE dev_tmp.cjh_7click_dlc_1026 AS
SELECT
	lower(trim(pin)) as pin
FROM dwb.dwb_brs_jr_web_click_fin_log_clstag_i_d
WHERE dt>=date_add('2018-11-28',-7)
  AND dt<=date_add('2018-11-28',-1)
  and length(lower(trim(pin)))>0
group by lower(trim(pin))
;

--一年内每个用户每日多种产品持仓数字总和
DROP TABLE IF EXISTS dev_tmp.cjh_dlc_1026_1y;
CREATE TABLE dev_tmp.cjh_dlc_1026_1y AS
SELECT
	lower(trim(user_pin)) as user_pin,
	sum(hold_amt_day) as hold_amt_day_all,
	dt
FROM dws.dws_fin_user_hold_i_d
WHERE dt<=date_add('2018-11-28',-1) and dt>=date_add('2018-11-28',-365) and length(lower(trim(user_pin)))>0
group by lower(trim(user_pin)),dt
;

--一年内用户单天最高持仓表
DROP TABLE IF EXISTS dev_tmp.cjh_maxdlc_1026_1y;
CREATE TABLE dev_tmp.cjh_maxdlc_1026_1y AS
SELECT
	lower(trim(user_pin)) as user_pin,
	max(hold_amt_day_all) as max_hold_amt_day_all
FROM dev_tmp.cjh_dlc_1026_1y
WHERE length(lower(trim(user_pin)))>0
group by lower(trim(user_pin))
;

--一年持仓大于10W用户名单
DROP TABLE IF EXISTS dev_tmp.cjh_dlc10W_user;
CREATE TABLE dev_tmp.cjh_dlc10W_user AS
select
	lower(trim(user_pin)) as user_pin
from dev_tmp.cjh_maxdlc_1026_1y
where length(lower(trim(user_pin)))>0 and max_hold_amt_day_all>100000
group by lower(trim(user_pin))
;

--5标签下用户名单(基金单购入，基金单预约，三十天点击股票埋点，持仓10W，30天东家金服浏览)
DROP TABLE IF EXISTS dev_tmp.cjh_all_label_user;
CREATE TABLE dev_tmp.cjh_all_label_user AS
select
	lower(trim(md.acct)) as acct
from dev_tmp.cjh_model_pred_list_1122_350W md
inner join
(select lower(trim(all_u.pin)) as pin from
	(select pin from dev_tmp.cjh_jjd_user
	 union all
	 select pin from dev_tmp.cjh_jjyy_user
	 union all
	 select pin from dev_tmp.cjh_30click_stock_user
	 union all
	 select user_pin as pin from dev_tmp.cjh_dlc10W_user
	 union all
	 select pin from dev_tmp.cjh_djjf_30view_user) all_u
) all_u
on lower(trim(md.acct))=lower(trim(all_u.pin))
group by lower(trim(md.acct))
;

--实验组1 和模型预测名单再匹配的全部名单(按顺序排列，共43W 可用limit)
DROP TABLE IF EXISTS dev_tmp.cjh_exp_firstgroup_user;
CREATE TABLE dev_tmp.cjh_exp_firstgroup_user AS
select
	lower(trim(t.acct)) as acct,
	idx
from dev_tmp.cjh_all_label_user t
inner join dev_tmp.cjh_model_pred_list_1122_350W md
on lower(trim(md.acct))=lower(trim(t.acct))
group by lower(trim(t.acct)),idx
order by idx asc
;

--实验组去除1106发送名单（约40W）
DROP TABLE IF EXISTS dev_tmp.cjh_exp_firstgroup_user_nosend;
CREATE TABLE dev_tmp.cjh_exp_firstgroup_user_nosend AS
select
	lower(trim(t.acct)) as acct,
	t.idx
from dev_tmp.cjh_exp_firstgroup_user t
left join dev_tmp.cjh_send_1106 send
on lower(trim(t.acct))=lower(trim(send.pin))
where (send.pin is null)
group by lower(trim(t.acct)),t.idx
order by t.idx asc
;

--实验组2，2标签下用户名单（7活加可投资50W）-去除1106发送
DROP TABLE IF EXISTS dev_tmp.cjh_2_label_user;
CREATE TABLE dev_tmp.cjh_2_label_user AS
select
	lower(trim(md.acct)) as acct,
	idx
from dev_tmp.cjh_model_pred_list_1122_350W md
inner join dev_tmp.cjh_7active_1026 act
on lower(trim(md.acct))= lower(trim(act.pin))
inner join dev_tmp.cjh_property50W_user pro
on lower(trim(md.acct))=lower(trim(pro.user_id))
left join dev_tmp.cjh_send_1106 send
on lower(trim(md.acct))=lower(trim(send.pin))
where (send.pin is null) and length(lower(trim(md.acct)))>0
group by lower(trim(md.acct)),idx
order by idx asc
limit 20000
;

--实验组1与实验组2合并
DROP TABLE IF EXISTS dev_tmp.cjh_exp_finalall_user;
CREATE TABLE dev_tmp.cjh_exp_finalall_user AS
select lower(trim(t.acct)) as acct
from
	(select acct from dev_tmp.cjh_exp_firstgroup_user_nosend
	 union all
	 select acct from dev_tmp.cjh_2_label_user) t
group by lower(trim(t.acct))
;

--对照组1 可投资产大于50W七天活跃且不在我们名单中的用户(341509人)
DROP TABLE IF EXISTS dev_tmp.cjh_property50W_7active_user;
CREATE TABLE dev_tmp.cjh_property50W_7active_user AS
select
	t.pin,
	rand()*100000 AS sample_idx
from dev_tmp.cjh_7active_1026 t
inner join dev_tmp.cjh_property50W_user pro
on lower(trim(t.pin))= lower(trim(pro.user_id))
left join dev_tmp.cjh_exp_finalall_user fir
on lower(trim(t.pin))=lower(trim(fir.acct))
where (fir.acct is null) and length(lower(trim(pro.user_id)))>0
order by sample_idx asc
limit 10000
;

--对照组2 可投资资产大于50W
DROP TABLE IF EXISTS dev_tmp.cjh_property50W_final_user;
CREATE TABLE dev_tmp.cjh_property50W_final_user AS
select
	t.user_id as pin,
	rand()*100000 AS sample_idx2
from dev_tmp.cjh_property50W_user t
left join dev_tmp.cjh_exp_finalall_user fir
on lower(trim(t.user_id))=lower(trim(fir.acct))
left join dev_tmp.cjh_property50W_7active_user pa
on lower(trim(t.user_id))=lower(trim(pa.pin))
where (fir.acct is null) and (pa.pin is null) and length(lower(trim(t.user_id)))>0
order by sample_idx2 asc
limit 10000
;

--对照组3 七天活跃
DROP TABLE IF EXISTS dev_tmp.cjh_7active_final_user;
CREATE TABLE dev_tmp.cjh_7active_final_user AS
select
	t.pin,
	rand()*100000 AS sample_idx3
from dev_tmp.cjh_7active_1026 t
left join dev_tmp.cjh_exp_finalall_user fir
on lower(trim(t.pin))=lower(trim(fir.acct))
left join dev_tmp.cjh_property50W_7active_user pa
on lower(trim(t.pin))=lower(trim(pa.pin))
left join dev_tmp.cjh_property50W_final_user pro
on lower(trim(t.pin))=lower(trim(pro.pin))
where (fir.acct is null) and (pa.pin is null) and (pro.pin is null) 
	and length(lower(trim(t.pin)))>0
order by sample_idx3 asc
limit 10000
;

--对照组商城
DROP TABLE IF EXISTS dev_tmp.cjh_contrast_sc_user;
CREATE TABLE dev_tmp.cjh_contrast_sc_user AS
select
	t.acct,
	t.idx
from dev_tmp.mall_contrast_1122 t
left join dev_tmp.cjh_exp_finalall_user fir
on lower(trim(t.acct))=lower(trim(fir.acct))
left join dev_tmp.cjh_property50W_7active_user pa
on lower(trim(t.acct))=lower(trim(pa.pin))
left join dev_tmp.cjh_property50W_final_user pro
on lower(trim(t.acct))=lower(trim(pro.pin))
left join dev_tmp.cjh_7active_final_user act
on lower(trim(t.acct))=lower(trim(act.pin))
where (fir.acct is null) and (pa.pin is null)
	and (pro.pin is null) and (act.pin is null)
	and length(lower(trim(t.acct)))>0
order by t.idx asc
limit 10000
;

--最终提交名单（实验组.对照组整合）
DROP TABLE IF EXISTS dev_tmp.cjh_final_userlist;
CREATE TABLE dev_tmp.cjh_final_userlist AS
select lower(trim(t.pin)) as pin
from 
	(select acct as pin from dev_tmp.cjh_exp_finalall_user
	 union all
	 select pin from dev_tmp.cjh_property50W_7active_user
	 union all
	 select pin from dev_tmp.cjh_property50W_final_user
	 union all
	 select pin from dev_tmp.cjh_7active_final_user
	 union all
	 select acct as pin from dev_tmp.cjh_contrast_sc_user) t
where length(lower(trim(t.pin)))>0
group by lower(trim(t.pin))
;

--从猛犸系统拉取表格准备
DROP TABLE IF EXISTS dev_tmp.cjh_final_userlist_trans;
CREATE TABLE dev_tmp.cjh_final_userlist_trans AS
SELECT trans.flag,
       concat_ws('&,', collect_set(trans.pin)) AS pin
FROM
	(SELECT t.pin,
          1 AS flag
     FROM dev_tmp.cjh_final_userlist t
     GROUP BY t.pin
	) trans
GROUP BY trans.flag
;

----------------统计SQL由此开始------------------------

--一年内基金单用户模型匹配预约情况
DROP TABLE IF EXISTS dev_tmp.cjh_pred_jjd_yy_350W_stat;
CREATE TABLE dev_tmp.cjh_pred_jjd_yy_350W_stat AS
select
	count(t.pin) as jjd_pred_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as jjd_pred_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as jjd_pred_yy28_cnt
from dev_tmp.cjh_jjd_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
inner join dev_tmp.cjh_model_pred_list_1122_350W md
on lower(trim(t.pin))=lower(trim(md.acct))
;

--基金预约与模型匹配的预约统计
DROP TABLE IF EXISTS dev_tmp.cjh_pred_jjyy_yy_350W_stat;
CREATE TABLE dev_tmp.cjh_pred_jjyy_yy_350W_stat AS
select
	count(t.pin) as jjyy_pred_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as jjyy_pred_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as jjyy_pred_yy28_cnt
from dev_tmp.cjh_jjyy_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
inner join dev_tmp.cjh_model_pred_list_1122_350W md
on lower(trim(t.pin))=lower(trim(md.acct))
;

--30天股票财迷点击模型匹配预约匹配统计
DROP TABLE IF EXISTS dev_tmp.cjh_pred_30click_stock_yy_350W_stat;
CREATE TABLE dev_tmp.cjh_pred_30click_stock_yy_350W_stat AS
select
	count(t.pin) as stock_click30_pred_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as stock_click30_pred_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as stock_click30_pred_yy28_cnt
from dev_tmp.cjh_30click_stock_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
inner join dev_tmp.cjh_model_pred_list_1122_350W md
on lower(trim(t.pin))=lower(trim(md.acct))
;

--一年内持仓超过N W并且预约的用户模型匹配统计
drop table if exists dev_tmp.cjh_dlc_yy_model_350W_stat;
create table dev_tmp.cjh_dlc_yy_model_350W_stat
as
select 
	sum(case when t.max_hold_amt_day_all>100000
			 then 1
			 else 0
		end) as dlc_10W_cnt,
	sum(case when yy.yytime ='2018-10-27' and t.max_hold_amt_day_all>100000
			 then 1
			 else 0
	    end) as dlc10W_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28' and t.max_hold_amt_day_all>100000
			 then 1
			 else 0
	    end) as dlc10W_yy28_cnt
from dev_tmp.cjh_maxdlc_1026_1y t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.user_pin))=lower(trim(yy.acct))
inner join dev_tmp.cjh_model_pred_list_1122_350W md
on lower(trim(t.user_pin))=lower(trim(md.acct))
where length(lower(trim(t.user_pin)))>0
;

--30天浏览东家财富页面用户模型匹配预约统计
DROP TABLE IF EXISTS dev_tmp.cjh_pred_djjf_30view_yy_350W_stat;
CREATE TABLE dev_tmp.cjh_pred_djjf_30view_yy_350W_stat AS
select
	count(t.pin) as djjf_30view_pred_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as djjf_30view_pred_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as djjf_30view_pred_yy28_cnt
from dev_tmp.cjh_djjf_30view_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
inner join dev_tmp.cjh_model_pred_list_1122_350W md
on lower(trim(t.pin))=lower(trim(md.acct))
;

--实验组1与实验组二重合人数统计
DROP TABLE IF EXISTS dev_tmp.cjh_exp1exp2_userstat;
CREATE TABLE dev_tmp.cjh_exp1exp2_userstat AS
select
	count(lower(trim(exp2.acct))) as exp2_cnt,
	sum(case when (exp1.acct is not null)
			 then 1
			 else 0
		end) as exp1exp2_cnt
from dev_tmp.cjh_2_label_user exp2
left join dev_tmp.cjh_exp_firstgroup_user_nosend exp1
on lower(trim(exp2.acct)) = lower(trim(exp1.acct))
;

--1106发送名单与实验组1占比统计(43W中有3W重复)
DROP TABLE IF EXISTS dev_tmp.cjh_send1106_exper_stat;
CREATE TABLE dev_tmp.cjh_send1106_exper_stat AS
select
	count(t.acct) as exp_cnt,
	sum(case when (send.pin is not null)
			 then 1
			 else 0
		end) as exp_send_cnt
from dev_tmp.cjh_exp_firstgroup_user t
left join dev_tmp.cjh_send_1106 send
on lower(trim(t.acct))=lower(trim(send.pin))
;

--去除1106发送的名单后预约情况统计
DROP TABLE IF EXISTS dev_tmp.cjh_pred_exp1_nosend_stat;
CREATE TABLE dev_tmp.cjh_pred_exp1_nosend_stat AS
select
	count(t.acct) as exp1nosend_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as exp1nosend_cnt_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as exp1nosend_cnt_yy28_cnt
from dev_tmp.cjh_exp_firstgroup_user_nosend t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.acct))=lower(trim(yy.acct))
;

--实验组2预约统计情况
DROP TABLE IF EXISTS dev_tmp.cjh_pred_exp2_nosend_stat;
CREATE TABLE dev_tmp.cjh_pred_exp2_nosend_stat AS
select
	count(t.acct) as exp2nosend_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as exp1nosend_cnt_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as exp1nosend_cnt_yy28_cnt
from dev_tmp.cjh_2_label_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.acct))=lower(trim(yy.acct))
;

--实验组总的预约统计情况
DROP TABLE IF EXISTS dev_tmp.cjh_pred_expall_stat;
CREATE TABLE dev_tmp.cjh_pred_expall_stat AS
select
	count(t.acct) as exp1nosend_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as exp1nosend_cnt_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as exp1nosend_cnt_yy28_cnt
from dev_tmp.cjh_exp_finalall_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.acct))=lower(trim(yy.acct))
;

--对照组1预约统计情况
DROP TABLE IF EXISTS dev_tmp.cjh_pred_cont1_stat;
CREATE TABLE dev_tmp.cjh_pred_cont1_stat AS
select
	count(t.pin) as cont1_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as cont1_cnt_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as cont1_cnt_yy28_cnt
from dev_tmp.cjh_property50W_7active_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
;

--对照组2预约统计情况
DROP TABLE IF EXISTS dev_tmp.cjh_pred_cont2_stat;
CREATE TABLE dev_tmp.cjh_pred_cont2_stat AS
select
	count(t.pin) as cont2_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as cont2_cnt_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as cont2_cnt_yy28_cnt
from dev_tmp.cjh_property50W_final_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
;

--对照组3预约统计情况
DROP TABLE IF EXISTS dev_tmp.cjh_pred_cont3_stat;
CREATE TABLE dev_tmp.cjh_pred_cont3_stat AS
select
	count(t.pin) as cont3_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as cont3_cnt_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as cont3_cnt_yy28_cnt
from dev_tmp.cjh_7active_final_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
;

--商城对照组预约情况统计
DROP TABLE IF EXISTS dev_tmp.cjh_pred_sc_cont_stat;
CREATE TABLE dev_tmp.cjh_pred_sc_cont_stat AS
select
	count(t.acct) as sc_cont_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as sc_cont_cnt_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as sc_cont_cnt_yy28_cnt
from dev_tmp.cjh_contrast_sc_user t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.acct))=lower(trim(yy.acct))
;

--最终全名单预约情况统计
DROP TABLE IF EXISTS dev_tmp.cjh_pred_all_stat;
CREATE TABLE dev_tmp.cjh_pred_all_stat AS
select
	count(t.pin) as all_cnt,
	sum(case when yy.yytime ='2018-10-27'
			 then 1
			 else 0
		end) as all_cnt_yy27_cnt,
	sum(case when yy.yytime ='2018-10-28'
			 then 1
			 else 0
		end) as all_cnt_yy28_cnt
from dev_tmp.cjh_final_userlist t
left join dev_tmp.cjh_djjf_yy2728 yy
on lower(trim(t.pin))=lower(trim(yy.acct))
;
