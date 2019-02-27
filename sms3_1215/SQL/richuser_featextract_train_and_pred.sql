
--当前日期为:'2018-12-13'

--1 构造用户信息和购物信息基本表
--（1）用户信息基本表
drop table if exists tmp.zqp_userinfo_feature;
create table tmp.zqp_userinfo_feature 
as
select 
	lower(trim(user_log_acct)) as acct,
	max(member_reg_gender) as gender,
	max(cast(datediff('2018-12-13',to_date(reg_birthday)) as int)/365) as age,
	min(datediff('2018-12-13',to_date(user_reg_tm))) as reg_days,
	min(datediff('2018-12-13',to_date(last_login_tm))) as last_login_days,
	min(datediff('2018-12-13',to_date(last_create_ord_tm))) as last_ord_days,
	max(user_lv_cd) as user_lv_cd,
	max(to_date(user_reg_tm)) as user_reg_tm,
	max(reg_birthday) as reg_birthday,
	max(reg_user_type_cd) as reg_user_type_cd,
	max(last_create_ord_tm) as last_create_ord_tm
from gdm.gdm_m01_userinfo_basic_sum
where dt = date_add('2018-12-13', -1) and length(lower(trim(user_log_acct)))>0
group by lower(trim(user_log_acct))
;
	
--（2）购物信息基本表--1 year
drop table if exists tmp.zqp_order_1year;
create table tmp.zqp_order_1year
as
select
	lower(trim(t.user_log_acct)) as acct,
	t.parent_sale_ord_id,
	t.sale_ord_id,
	t.sale_ord_tm,
	t.item_id,
	t.item_name,
	t.brandname,
	t.sale_qtty,
	t.item_first_cate_name,
	t.item_second_cate_name,
	t.item_third_cate_name,
	t.before_prefr_unit_price,
	t.after_prefr_unit_price,
	t.user_actual_pay_amount,
	(case when t.sale_ord_valid_flag=1
		  then 1
		  else 0
		  end) as sale_ord_valid_flag,
	(case when t.cancel_flag=1
	      then 1
	      else 0
		  end) as cancel_flag,
	t.total_offer_amount,
	t.ord_flag,
	t.user_lv_cd,
	t.reg_user_type_cd,
	t.item_sku_id,
    LG.barndname_full,
	(case when LG.barndname_full is NULL
	      then 0
	      else 1
		  end) as luxury_goods_flag
from gdm.gdm_m04_ord_det_sum t
left join tmp.tw_LuxuryGoods LG
on t.item_sku_id=LG.item_sku_id
where t.dt >= date_add('2018-12-13', -365) and t.sale_ord_dt >= date_add('2018-12-13',-365) and t.sale_ord_dt <= date_add('2018-12-13',-1) 
	and length(lower(trim(t.user_log_acct)))>0
;


-- (3) 分期表
drop table if exists tmp.zqp_installment_1year;
create table tmp.zqp_installment_1year
as
select 
	lower(trim(pin)) as pin,
    sum(case when instalmentnum>0
        	 then paymoney
        	 else 0
			 end) as installment_1year_amt,
    sum(case when instalmentnum>0
        	 then 1
        	 else 0
			 end) as installment_1year_cnt,
    sum(case when instalmentnum>=2
        	 then paymoney
        	 else 0
			 end) as installment2_1year_amt,  
    sum(case when instalmentnum>=2
        	 then 1
        	 else 0
			 end) as installment2_1year_cnt, 
    sum(case when instalmentnum>=6
        	 then paymoney
        	 else 0
			 end) as installment6_1year_amt,  
    sum(case when instalmentnum>=6
        	 then 1
        	 else 0
			 end) as installment6_1year_cnt
from fdm.fdm_paytrade_02_payresult_chain  
where dp in ("ACTIVE",'HISTORY') and dt>=date_add('2018-12-13', -365) and length(lower(trim(pin)))>0
	and  paytime>=date_add('2018-12-13', -365) and paytime<=date_add('2018-12-13', -1)
group by lower(trim(pin))
;



--（4）购物信息特征表
drop table if exists tmp.zqp_order_1year_feature;
create table tmp.zqp_order_1year_feature
as
select
   acct,
   sum(case when sale_ord_valid_flag=1
		    then user_actual_pay_amount
		    else 0
	   end) as order_1year_amt,
   count(distinct parent_sale_ord_id) as order_1year_cnt,
   avg(case when sale_ord_valid_flag=1
		    then user_actual_pay_amount
		    else 0
	   end) as goods_mean_1year,
   max(case when sale_ord_valid_flag=1
		    then user_actual_pay_amount
		    else 0
	   end) as goods_max_1year,
   sum(case when substring(ord_flag,40,1) in (1,2,3,4,5,6,7)
            then 1
			else 0
			end) as ord_ent_cnt,
   sum(case when sale_ord_valid_flag=1
		    then 1
		    else 0
			end) as valid_1year_cnt,
   sum(case when sale_ord_valid_flag=0
		    then 1
		    else 0
			end) as invalid_1year_cnt,
   sum(case when cancel_flag=1
		    then 1
		    else 0
			end) as cancel_1year_cnt,
   sum(case when sale_ord_valid_flag=1
		    then total_offer_amount
		    else 0
	        end) as offer_1year_amt,
   -- 火车
   sum(case when item_second_cate_name='地面交通票务' and item_third_cate_name='国内火车票' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as train_1year_amt,
   count(distinct case when item_second_cate_name='地面交通票务' and item_third_cate_name='国内火车票' and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as train_1year_cnt,
   sum(case when item_second_cate_name='地面交通票务' and item_third_cate_name='火车保险' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as train_insure_1year_amt,
   count(distinct case when item_second_cate_name='地面交通票务' and item_third_cate_name='火车保险' and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as train_insure_1year_cnt,
   -- 飞机
   sum(case when item_second_cate_name='机票预定' and item_third_cate_name='国内机票' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as domestic_flight_1year_amt,
   count(distinct case when item_second_cate_name='机票预定' and item_third_cate_name='国内机票' and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as domestic_flight_1year_cnt,	   
   sum(case when item_second_cate_name='机票预定' and item_third_cate_name='国际机票' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as internation_flight_insure_1year_amt,
   count(distinct case when item_second_cate_name='机票预定' and item_third_cate_name='国际机票' and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as internation_flight_1year_cnt,			   
   sum(case when item_second_cate_name='机票预定' and  item_third_cate_name='机票套餐' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as return_flight_1year_amt,
   count(distinct case when item_second_cate_name='机票预定' and  item_third_cate_name='机票套餐' and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as return_flight_1year_cnt,		   
   sum(case when item_second_cate_name='机票预定' and item_third_cate_name in ('机票套餐','国际机票','国内机票')  and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as flight_1year_amt,
   count(distinct case when item_second_cate_name='机票预定' and item_third_cate_name in ('机票套餐','国际机票','国内机票') and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as flight_1year_cnt,
   sum(case when item_second_cate_name='机票预定' and item_third_cate_name='机场服务' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as service_flight_1year_amt,
   count(distinct case when item_second_cate_name='机票预定' and item_third_cate_name='机场服务' and sale_ord_valid_flag=1
					    then parent_sale_ord_id
				        else NULL
					    end) as service_flight_1year_cnt,					   		   
   -- 酒类
   sum(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1    -- count(col) will ignore NULL 
			then user_actual_pay_amount
			end) as wine_1year_amt,
   count(distinct case when item_first_cate_name='酒类' and sale_ord_valid_flag=1    
					   then parent_sale_ord_id
				       else NULL
					   end) as wine_1year_cnt,
   max(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1         
			then after_prefr_unit_price
			end) as wine_1year_max_price,
   avg(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1         
			then after_prefr_unit_price
			end) as wine_1year_mean_price,
    -- 高档酒类
   sum(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1  and brandname in ('茅台','五粮液','洋河','苏菲') and after_prefr_unit_price>600
			then user_actual_pay_amount
			end) as high_wine_1year_amt,	
   count(distinct case when item_first_cate_name='酒类' and sale_ord_valid_flag=1 and brandname in ('茅台','五粮液','洋河','苏菲') and after_prefr_unit_price>600
				       then user_actual_pay_amount
						end) as high_wine_1year_cnt,
   max(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1 and brandname in  ('茅台','五粮液','洋河','苏菲') and after_prefr_unit_price>600
			then after_prefr_unit_price
			end) as high_wine_1year_max_price,	
   avg(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1 and brandname in  ('茅台','五粮液','洋河','苏菲') and after_prefr_unit_price>600      
			then after_prefr_unit_price
			end) as high_wine_1year_mean_price,	
	-- 珠宝首饰
   sum(case when item_first_cate_name='珠宝首饰' and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as jel_1year_amt,
   count(distinct case when item_first_cate_name='珠宝首饰' and sale_ord_valid_flag=1    
					   then parent_sale_ord_id
				       else NULL
					   end) as jel_1year_cnt,
   max(case when item_first_cate_name='珠宝首饰' and sale_ord_valid_flag=1         
			then after_prefr_unit_price
		    end) as jel_1year_max_price,
   avg(case when item_first_cate_name='珠宝首饰' and sale_ord_valid_flag=1         
			then after_prefr_unit_price
		    end) as jel_1year_mean_price,	
   -- 奢侈品
   sum(case when luxury_goods_flag=1 and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as lg_1year_amt,
   count(distinct case when luxury_goods_flag=1 and sale_ord_valid_flag=1     
					   then parent_sale_ord_id
				       else NULL
					   end) as lg_1year_cnt,
   max(case when luxury_goods_flag=1 and sale_ord_valid_flag=1       
			then after_prefr_unit_price
		    end) as lg_1year_max_price,
   avg(case when luxury_goods_flag=1 and sale_ord_valid_flag=1           
			then after_prefr_unit_price
			end) as lg_1year_mean_price,		
   -- 手机消费
   sum(case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as tel_1year_amt,
   count(distinct case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as tel_1year_cnt,
   count(distinct case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1 and user_actual_pay_amount>100     
					   then parent_sale_ord_id
				       else NULL
					   end) as tel_100_1year_cnt,
   count(distinct case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1 and user_actual_pay_amount>200   
					   then parent_sale_ord_id
				       else NULL
					   end) as tel_200_1year_cnt,
   max(case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1         
			then after_prefr_unit_price
		    end) as tel_1year_max_price,
   avg(case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1         
			then after_prefr_unit_price
		    end) as tel_1year_mean_price,
   
   -- 京东卡
   sum(case when (item_third_cate_name='京东卡' or  brandname='京东E卡') and sale_ord_valid_flag=1   
			then user_actual_pay_amount
			end) as jdcard_1year_amt,
   count(distinct case when item_third_cate_name='京东卡' or  brandname='京东E卡'
					   then parent_sale_ord_id
				       else NULL
					   end) as jdcard_1year_cnt,
	
	-- 拍卖
   sum(case when item_first_cate_name='拍卖' and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as auction_1year_amt,
   count(distinct case when item_first_cate_name='拍卖'
					   then parent_sale_ord_id
				       else NULL
					   end) as auction_1year_cnt,	
   -- 保证金商品
   sum(case when REGEXP(item_name,'保证金商品') and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as margin_1year_amt,
   count(distinct case when REGEXP(item_name,'保证金商品')
					   then parent_sale_ord_id
				       else NULL
					   end) as margin_1year_cnt,	
   -- 艺术品
   sum(case when (item_first_cate_name in ('邮币','艺术品') or item_third_cate_name in ('装饰字画','收藏品','古董文玩','书法'))  and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as art_1year_amt,
   count(distinct case when (item_first_cate_name in ('邮币','艺术品') or item_third_cate_name in ('装饰字画','收藏品','古董文玩','书法'))  
					   then parent_sale_ord_id
				       else NULL
					   end) as art_1year_cnt					   
from tmp.zqp_order_1year
group by acct
;

--（5）综合购物信息和基本信息---需要处理各种NULL或者异常
drop table if exists tmp.zqp_userinfo_order_1year_feature;
create table tmp.zqp_userinfo_order_1year_feature
as
select 
	t.acct,
	(case when t.gender in (0,1) 
	      then t.gender
	      else 2
		  end) as gender,
	(case when age>0 and age<99 
		  then age
		  else -1
		  end) as age,
	t.reg_days,
	(case when t.user_lv_cd in (10,33,34,50,56,61,62,90,105,110) 
		  then t.user_lv_cd
		  else -1
		  end) as user_lv_cd,
	t.reg_user_type_cd,
	t.last_create_ord_tm,
	nvl(t.last_login_days,-1) as last_login_days,
	nvl(t.last_ord_days,-1) as last_ord_days,
	nvl(shop.order_1year_amt,0) as order_1year_amt,
	shop.order_1year_cnt,
	nvl(shop.goods_mean_1year,0) as goods_mean_1year,
	nvl(shop.goods_max_1year,0) as goods_max_1year,
	shop.valid_1year_cnt,
	shop.invalid_1year_cnt,
	shop.cancel_1year_cnt,
	nvl(shop.offer_1year_amt,0) as offer_1year_amt,
	nvl(shop.train_1year_amt,0) as train_1year_amt,
	shop.train_1year_cnt,	
	nvl(shop.train_insure_1year_amt,0) as train_insure_1year_amt,
	shop.train_insure_1year_cnt,
	nvl(shop.domestic_flight_1year_amt,0) as domestic_flight_1year_amt,
	shop.domestic_flight_1year_cnt,	
	nvl(shop.internation_flight_insure_1year_amt,0) as internation_flight_insure_1year_amt,
	shop.internation_flight_1year_cnt,	
	nvl(shop.return_flight_1year_amt,0) as return_flight_1year_amt,
	shop.return_flight_1year_cnt,	
	nvl(shop.flight_1year_amt,0) as flight_1year_amt,
	shop.flight_1year_cnt,
	nvl(shop.service_flight_1year_amt,0) as service_flight_1year_cnt,
	nvl(shop.wine_1year_amt,0) as wine_1year_amt,
	shop.wine_1year_cnt,
	nvl(shop.wine_1year_max_price,0) as wine_1year_max_price,
	nvl(shop.wine_1year_mean_price,0) as wine_1year_mean_price,
	nvl(shop.high_wine_1year_amt,0) as high_wine_1year_amt,
	shop.high_wine_1year_cnt,
	nvl(shop.high_wine_1year_max_price,0) as high_wine_1year_max_price,
	nvl(shop.high_wine_1year_mean_price,0) as high_wine_1year_mean_price,
	nvl(shop.jel_1year_amt,0) as jel_1year_amt,
	shop.jel_1year_cnt,
	nvl(shop.jel_1year_max_price,0) as jel_1year_max_price,
	nvl(shop.jel_1year_mean_price,0) as jel_1year_mean_price,
	nvl(shop.lg_1year_amt,0) as lg_1year_amt,
	shop.lg_1year_cnt,
	nvl(shop.lg_1year_max_price,0) as lg_1year_max_price,
	nvl(shop.lg_1year_mean_price,0) as lg_1year_mean_price,
	nvl(shop.tel_1year_amt,0) as tel_1year_amt,
	shop.tel_1year_cnt,
	shop.tel_100_1year_cnt,
	shop.tel_200_1year_cnt,
	nvl(shop.tel_1year_max_price,0) as tel_1year_max_price,
	nvl(shop.tel_1year_mean_price,0) as tel_1year_mean_price,
	shop.jdcard_1year_amt,
	shop.jdcard_1year_cnt,
	nvl(shop.auction_1year_amt,0) as auction_1year_amt,
	shop.auction_1year_cnt,
	nvl(shop.margin_1year_amt,0) as margin_1year_amt,
	shop.margin_1year_cnt,	
	nvl(shop.art_1year_amt,0) as art_1year_amt,
	nvl(shop.art_1year_cnt,0) as art_1year_cnt,
	nvl(inst.installment_1year_amt,0) as installment_1year_amt,
	nvl(inst.installment_1year_cnt,0) as installment_1year_cnt,
	nvl(inst.installment2_1year_amt,0) as installment2_1year_amt,
	nvl(inst.installment2_1year_cnt,0) as installment2_1year_cnt,
	nvl(inst.installment6_1year_amt,0) as installment6_1year_amt,
	nvl(inst.installment6_1year_cnt,0) as installment6_1year_cnt
from tmp.zqp_userinfo_feature t
inner join tmp.zqp_order_1year_feature shop
on t.acct=shop.acct
left join tmp.zqp_installment_1year inst
on t.acct=inst.pin
;

-- (6) 购物基本表--5year
drop table if exists tmp.zqp_order_5year_acctall;
create table tmp.zqp_order_5year_acctall
as
select 
	lower(trim(user_log_acct)) as pin,
	parent_sale_ord_id,
	sale_ord_id,
	sale_ord_tm,
	item_id,
	item_name,
	brandname,
	sale_qtty,
	item_first_cate_name,
	item_second_cate_name,
	item_third_cate_name,
	before_prefr_unit_price,
	after_prefr_unit_price,
	user_actual_pay_amount,
	(case when sale_ord_valid_flag=1
		  then 1
		  else 0
		  end) as sale_ord_valid_flag,
	(case when cancel_flag=1
		  then 1
		  else 0
		  end) as cancel_flag,
	total_offer_amount,
	ord_flag,
	user_lv_cd,
	reg_user_type_cd,
	item_sku_id
from gdm.gdm_m04_ord_det_sum
where dt >= date_add('2018-12-13', -365*5) and sale_ord_dt >= date_add('2018-12-13', -365*5) and sale_ord_dt <= date_add('2018-12-13', -1) 
	and length(lower(trim(user_log_acct)))>0
;


--购物信息基本表+luxury_goods_flag    Time taken: 9428.997 seconds
drop table if exists tmp.zqp_order_5year_acctall_lg;
create table tmp.zqp_order_5year_acctall_lg
as
select
	t.*,
    LG.barndname_full,
	(case when LG.barndname_full is NULL
	      then 0
	      else 1
		  end) as luxury_goods_flag
from tmp.zqp_order_5year_acctall t
left join tmp.tw_LuxuryGoods LG
on t.item_sku_id=LG.item_sku_id
;



-- 购物信息特征表    Time taken: 2793.563 seconds
drop table if exists tmp.zqp_order_5year_feature_all_pin;
create table tmp.zqp_order_5year_feature_all_pin
as
select
   pin,
   sum(case when sale_ord_valid_flag=1
		    then user_actual_pay_amount
		    else 0
	   end) as order_5year_amt,
   count(distinct parent_sale_ord_id) as order_5year_cnt,
   avg(case when sale_ord_valid_flag=1
		    then user_actual_pay_amount
		    else 0
	   end) as goods_mean_5year,
   max(case when sale_ord_valid_flag=1
		    then user_actual_pay_amount
		    else 0
	   end) as goods_max_5year,
   sum(case when substring(ord_flag,40,1) in (1,2,3,4,5,6,7)
            then 1
			else 0
			end) as ord_ent_cnt,
   sum(case when sale_ord_valid_flag=1
		    then 1
		    else 0
			end) as valid_5year_cnt,
   sum(case when sale_ord_valid_flag=0
		    then 1
		    else 0
			end) as invalid_5year_cnt,
   sum(case when cancel_flag=1
		    then 1
		    else 0
			end) as cancel_5year_cnt,
   sum(case when sale_ord_valid_flag=1
		    then total_offer_amount
		    else 0
	        end) as offer_5year_amt,
   -- 火车
   sum(case when item_second_cate_name='地面交通票务' and item_third_cate_name='国内火车票' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as train_5year_amt,
   count(distinct case when item_second_cate_name='地面交通票务' and item_third_cate_name='国内火车票' and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as train_5year_cnt,
   sum(case when item_second_cate_name='地面交通票务' and item_third_cate_name='火车保险' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as train_insure_5year_amt,
   count(distinct case when item_second_cate_name='地面交通票务' and item_third_cate_name='火车保险' and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as train_insure_5year_cnt,
   -- 飞机
   sum(case when item_second_cate_name='机票预定' and item_third_cate_name='国内机票' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as domestic_flight_5year_amt,
   count(distinct case when item_second_cate_name='机票预定' and item_third_cate_name='国内机票' and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as domestic_flight_5year_cnt,	   
   sum(case when item_second_cate_name='机票预定' and item_third_cate_name='国际机票' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as internation_flight_insure_5year_amt,
   count(distinct case when item_second_cate_name='机票预定' and item_third_cate_name='国际机票' and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as internation_flight_5year_cnt,			   
   sum(case when item_second_cate_name='机票预定' and  item_third_cate_name='机票套餐' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as return_flight_5year_amt,
   count(distinct case when item_second_cate_name='机票预定' and  item_third_cate_name='机票套餐' and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as return_flight_5year_cnt,		   
   sum(case when item_second_cate_name='机票预定' and item_third_cate_name in ('机票套餐','国际机票','国内机票') and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as flight_5year_amt,
   count(distinct case when item_second_cate_name='机票预定' and item_third_cate_name in ('机票套餐','国际机票','国内机票') and sale_ord_valid_flag=1
					   then parent_sale_ord_id
				       else NULL
					   end) as flight_5year_cnt,
   sum(case when item_second_cate_name='机票预定' and item_third_cate_name='机场服务' and sale_ord_valid_flag=1
			then user_actual_pay_amount
			end) as service_flight_5year_amt,
   count(distinct case when item_second_cate_name='机票预定' and item_third_cate_name='机场服务' and sale_ord_valid_flag=1
					    then parent_sale_ord_id
				        else NULL
					    end) as service_flight_5year_cnt,					   		   
   -- 酒类
   sum(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1    -- count(col) will ignore NULL 
			then user_actual_pay_amount
			end) as wine_5year_amt,
   count(distinct case when item_first_cate_name='酒类' and sale_ord_valid_flag=1    
					   then parent_sale_ord_id
				       else NULL
					   end) as wine_5year_cnt,
   max(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1         
			then after_prefr_unit_price
			end) as wine_5year_max_price,
   avg(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1         
			then after_prefr_unit_price
			end) as wine_5year_mean_price,
    -- 高档酒类
   sum(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1  and brandname in ('茅台','五粮液','洋河','苏菲') and after_prefr_unit_price>600
			then user_actual_pay_amount
			end) as high_wine_5year_amt,	
   count(distinct case when item_first_cate_name='酒类' and sale_ord_valid_flag=1 and brandname in ('茅台','五粮液','洋河','苏菲') and after_prefr_unit_price>600
				       then user_actual_pay_amount
						end) as high_wine_5year_cnt,
   max(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1 and brandname in  ('茅台','五粮液','洋河','苏菲') and after_prefr_unit_price>600
			then after_prefr_unit_price
			end) as high_wine_5year_max_price,	
   avg(case when item_first_cate_name='酒类' and sale_ord_valid_flag=1 and brandname in  ('茅台','五粮液','洋河','苏菲') and after_prefr_unit_price>600      
			then after_prefr_unit_price
			end) as high_wine_5year_mean_price,	
	-- 珠宝首饰
   sum(case when item_first_cate_name='珠宝首饰' and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as jel_5year_amt,
   count(distinct case when item_first_cate_name='珠宝首饰' and sale_ord_valid_flag=1    
					   then parent_sale_ord_id
				       else NULL
					   end) as jel_5year_cnt,
   max(case when item_first_cate_name='珠宝首饰' and sale_ord_valid_flag=1         
			then after_prefr_unit_price
		    end) as jel_5year_max_price,
   avg(case when item_first_cate_name='珠宝首饰' and sale_ord_valid_flag=1         
			then after_prefr_unit_price
		    end) as jel_5year_mean_price,	
   -- 奢侈品
   sum(case when luxury_goods_flag=1 and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as lg_5year_amt,
   count(distinct case when luxury_goods_flag=1 and sale_ord_valid_flag=1     
					   then parent_sale_ord_id
				       else NULL
					   end) as lg_5year_cnt,
   max(case when luxury_goods_flag=1 and sale_ord_valid_flag=1       
			then after_prefr_unit_price
		    end) as lg_5year_max_price,
   avg(case when luxury_goods_flag=1 and sale_ord_valid_flag=1           
			then after_prefr_unit_price
			end) as lg_5year_mean_price,		
   -- 手机消费
   sum(case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as tel_5year_amt,
   count(distinct case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1    
					   then parent_sale_ord_id
				       else NULL
					   end) as tel_5year_cnt,
   count(distinct case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1 and user_actual_pay_amount>100     
					   then parent_sale_ord_id
				       else NULL
					   end) as tel_100_5year_cnt,
   count(distinct case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1 and user_actual_pay_amount>200   
					   then parent_sale_ord_id
				       else NULL
					   end) as tel_200_5year_cnt,
   max(case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1         
			then after_prefr_unit_price
		    end) as tel_5year_max_price,
   avg(case when (item_second_cate_name='通讯充值' or REGEXP(item_name,'手机充值|话费充值|流量充值')) and sale_ord_valid_flag=1         
			then after_prefr_unit_price
		    end) as tel_5year_mean_price,

	-- 京东卡
   sum(case when (item_third_cate_name='京东卡' or  brandname='京东E卡') and sale_ord_valid_flag=1   
			then user_actual_pay_amount
			end) as jdcard_5year_amt,
   count(distinct case when item_third_cate_name='京东卡' or  brandname='京东E卡'
					   then parent_sale_ord_id
				       else NULL
					   end) as jdcard_5year_cnt,
    -- 拍卖
   sum(case when item_first_cate_name='拍卖' and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as auction_5year_amt,
   count(distinct case when item_first_cate_name='拍卖'
					   then parent_sale_ord_id
				       else NULL
					   end) as auction_5year_cnt,	
   -- 保证金商品
   sum(case when REGEXP(item_name,'保证金商品') and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as margin_5year_amt,
   count(distinct case when REGEXP(item_name,'保证金商品')
					   then parent_sale_ord_id
				       else NULL
					   end) as margin_5year_cnt,	
   -- 艺术品
   sum(case when (item_first_cate_name in ('邮币','艺术品') or item_third_cate_name in ('装饰字画','收藏品','古董文玩','书法'))  and sale_ord_valid_flag=1    
			then user_actual_pay_amount
			end) as art_5year_amt,
   count(distinct case when (item_first_cate_name in ('邮币','艺术品') or item_third_cate_name in ('装饰字画','收藏品','古董文玩','书法'))
					   then parent_sale_ord_id
				       else NULL
					   end) as art_5year_cnt					   
from tmp.zqp_order_5year_acctall_lg
group by pin
;


-- 购物信息特征表限定为1年的客户    Time taken: 348.684 seconds
drop table if exists tmp.zqp_order_5year_feature_lastyear_pin;
create table tmp.zqp_order_5year_feature_lastyear_pin
as
select 
	ord5year.*
from tmp.zqp_order_1year_feature  t 
inner join tmp.zqp_order_5year_feature_all_pin ord5year
on lower(trim(t.acct))=lower(trim(ord5year.pin))
;



-- 分期表   Time taken: 808.71 seconds
drop table if exists tmp.zqp_installment_5year;
create table tmp.zqp_installment_5year
as
select 
	lower(trim(pin)) as instpin,
    sum(case when instalmentnum>0
        	 then paymoney
        	 else 0
			 end) as installment_5year_amt,
    sum(case when instalmentnum>0
        	 then 1
        	 else 0
			 end) as installment_5year_cnt,
    sum(case when instalmentnum>=2
        	 then paymoney
        	 else 0
			 end) as installment2_5year_amt,  
    sum(case when instalmentnum>=2
        	 then 1
        	 else 0
			 end) as installment2_5year_cnt, 
    sum(case when instalmentnum>=6
        	 then paymoney
        	 else 0
			 end) as installment6_5year_amt,  
    sum(case when instalmentnum>=6
        	 then 1
        	 else 0
			 end) as installment6_5year_cnt
from fdm.fdm_paytrade_02_payresult_chain  
where dp in ("ACTIVE",'HISTORY') and dt>=date_add('2018-12-13', -365*5) and length(lower(trim(pin)))>0
	and  paytime>=date_add('2018-12-13', -365*5) and paytime<=date_add('2018-12-13', -1)
group by lower(trim(pin))
;
 
 
-- 综合购物信息和分期表，同时处理字段类型及NULL     Time taken: 426.906 seconds
drop table if exists tmp.zqp_order_lastyear_pin_5year_feature;
create table tmp.zqp_order_lastyear_pin_5year_feature
as
select 
	shop.pin as pin,
	nvl(shop.order_5year_amt,0) as order_5year_amt,
	shop.order_5year_cnt,
	nvl(shop.goods_mean_5year,0) as goods_mean_5year,
	nvl(shop.goods_max_5year,0) as goods_max_5year,
	shop.valid_5year_cnt,
	shop.invalid_5year_cnt,
	shop.cancel_5year_cnt,
	nvl(shop.offer_5year_amt,0) as offer_5year_amt,
	nvl(shop.train_5year_amt,0) as train_5year_amt,
	shop.train_5year_cnt,	
	nvl(shop.train_insure_5year_amt,0) as train_insure_5year_amt,
	shop.train_insure_5year_cnt,
	nvl(shop.domestic_flight_5year_amt,0) as domestic_flight_5year_amt,
	shop.domestic_flight_5year_cnt,	
	nvl(shop.internation_flight_insure_5year_amt,0) as internation_flight_insure_5year_amt,
	shop.internation_flight_5year_cnt,	
	nvl(shop.return_flight_5year_amt,0) as return_flight_5year_amt,
	shop.return_flight_5year_cnt,	
	nvl(shop.flight_5year_amt,0) as flight_5year_amt,
	shop.flight_5year_cnt,
	nvl(shop.service_flight_5year_amt,0) as service_flight_5year_cnt,
	nvl(shop.wine_5year_amt,0) as wine_5year_amt,
	shop.wine_5year_cnt,
	nvl(shop.wine_5year_max_price,0) as wine_5year_max_price,
	nvl(shop.wine_5year_mean_price,0) as wine_5year_mean_price,
	nvl(shop.high_wine_5year_amt,0) as high_wine_5year_amt,
	shop.high_wine_5year_cnt,
	nvl(shop.high_wine_5year_max_price,0) as high_wine_5year_max_price,
	nvl(shop.high_wine_5year_mean_price,0) as high_wine_5year_mean_price,
	nvl(shop.jel_5year_amt,0) as jel_5year_amt,
	shop.jel_5year_cnt,
	nvl(shop.jel_5year_max_price,0) as jel_5year_max_price,
	nvl(shop.jel_5year_mean_price,0) as jel_5year_mean_price,
	nvl(shop.lg_5year_amt,0) as lg_5year_amt,
	shop.lg_5year_cnt,
	nvl(shop.lg_5year_max_price,0) as lg_5year_max_price,
	nvl(shop.lg_5year_mean_price,0) as lg_5year_mean_price,
	nvl(shop.tel_5year_amt,0) as tel_5year_amt,
	shop.tel_5year_cnt,
	shop.tel_100_5year_cnt,
	shop.tel_200_5year_cnt,
	nvl(shop.tel_5year_max_price,0) as tel_5year_max_price,
	nvl(shop.tel_5year_mean_price,0) as tel_5year_mean_price,
	shop.jdcard_5year_cnt,
	shop.jdcard_5year_amt,
	nvl(shop.auction_5year_amt,0) as auction_5year_amt,
	shop.auction_5year_cnt,
	nvl(shop.margin_5year_amt,0) as margin_5year_amt,
	shop.margin_5year_cnt,	
	nvl(shop.art_5year_amt,0) as art_5year_amt,
	nvl(shop.art_5year_cnt,0) as art_5year_cnt,
	nvl(inst.installment_5year_amt,0) as installment_5year_amt,
	nvl(inst.installment_5year_cnt,0) as installment_5year_cnt,
	nvl(inst.installment2_5year_amt,0) as installment2_5year_amt,
	nvl(inst.installment2_5year_cnt,0) as installment2_5year_cnt,
	nvl(inst.installment6_5year_amt,0) as installment6_5year_amt,
	nvl(inst.installment6_5year_cnt,0) as installment6_5year_cnt
from  tmp.zqp_order_5year_feature_lastyear_pin shop
left join tmp.zqp_installment_5year inst
on shop.pin=inst.instpin
;

-- (7) 综合1year和5year特征    Time taken: 418.875 seconds
drop table if exists tmp.zqp_userinfo_order_1and5year_feature_0;
create table tmp.zqp_userinfo_order_1and5year_feature_0
as
select 
	t.acct,
	cast(nvl(t.gender, 3) AS int) AS gender,
	nvl(t.age, -1) as age,
	nvl(t.reg_days, 36500) as reg_days,
	cast(nvl(t.user_lv_cd, -99) AS int) AS user_lv_cd,
	cast(nvl(t.reg_user_type_cd, -99) AS int) AS reg_user_type_cd,
	nvl(t.last_login_days, 36500) as last_login_days,
	nvl(t.last_ord_days, 36500) as last_ord_days,
	nvl(t.order_1year_amt, 0) as order_1year_amt,
	nvl(t.order_1year_cnt, 0) as order_1year_cnt,
	nvl(t.goods_mean_1year, 0) as goods_mean_1year,
	nvl(t.goods_max_1year, 0) as goods_max_1year,
	nvl(t.valid_1year_cnt, 0) as valid_1year_cnt,
	nvl(t.invalid_1year_cnt, 0) as invalid_1year_cnt,
	nvl(t.cancel_1year_cnt, 0) as cancel_1year_cnt,
	nvl(t.offer_1year_amt, 0) as offer_1year_amt,
	nvl(t.train_1year_amt, 0) as train_1year_amt,
	nvl(t.train_1year_cnt, 0) as train_1year_cnt,
	nvl(t.train_insure_1year_amt, 0) as train_insure_1year_amt,
	nvl(t.train_insure_1year_cnt, 0) as train_insure_1year_cnt,
	nvl(t.domestic_flight_1year_amt, 0) as domestic_flight_1year_amt,
	nvl(t.domestic_flight_1year_cnt, 0) as domestic_flight_1year_cnt,
	nvl(t.internation_flight_insure_1year_amt, 0) as internation_flight_insure_1year_amt,
	nvl(t.internation_flight_1year_cnt, 0) as internation_flight_1year_cnt,
	nvl(t.return_flight_1year_amt, 0) as return_flight_1year_amt,
	nvl(t.return_flight_1year_cnt, 0) as return_flight_1year_cnt,
	nvl(t.flight_1year_amt, 0) as flight_1year_amt,
	nvl(t.flight_1year_cnt, 0) as flight_1year_cnt,
	nvl(t.service_flight_1year_cnt, 0) as service_flight_1year_cnt,
	nvl(t.wine_1year_amt, 0) as wine_1year_amt,
	nvl(t.wine_1year_cnt, 0) as wine_1year_cnt,
	nvl(t.wine_1year_max_price, 0) as wine_1year_max_price,
	nvl(t.wine_1year_mean_price, 0) as wine_1year_mean_price,
	nvl(t.high_wine_1year_amt, 0) as high_wine_1year_amt,
	nvl(t.high_wine_1year_cnt, 0) as high_wine_1year_cnt,
	nvl(t.high_wine_1year_max_price, 0) as high_wine_1year_max_price,
	nvl(t.high_wine_1year_mean_price, 0) as high_wine_1year_mean_price,
	nvl(t.jel_1year_amt, 0) as jel_1year_amt,
	nvl(t.jel_1year_cnt, 0) as jel_1year_cnt,
	nvl(t.jel_1year_max_price, 0) as jel_1year_max_price,
	nvl(t.jel_1year_mean_price, 0) as jel_1year_mean_price,
	nvl(t.lg_1year_amt, 0) as lg_1year_amt,
	nvl(t.lg_1year_cnt, 0) as lg_1year_cnt,
	nvl(t.lg_1year_max_price, 0) as lg_1year_max_price,
	nvl(t.lg_1year_mean_price, 0) as lg_1year_mean_price,
	nvl(t.tel_1year_amt, 0) as tel_1year_amt,
	nvl(t.tel_1year_cnt, 0) as tel_1year_cnt,
	nvl(t.tel_100_1year_cnt, 0) as tel_100_1year_cnt,
	nvl(t.tel_200_1year_cnt, 0) as tel_200_1year_cnt,
	nvl(t.tel_1year_max_price, 0) as tel_1year_max_price,
	nvl(t.tel_1year_mean_price, 0) as tel_1year_mean_price,
	nvl(t.jdcard_1year_cnt, 0) as jdcard_1year_cnt,
	nvl(t.jdcard_1year_amt, 0) as jdcard_1year_amt,
	nvl(t.auction_1year_amt, 0) as auction_1year_amt,
	nvl(t.auction_1year_cnt, 0) as auction_1year_cnt,
	nvl(t.margin_1year_amt, 0) as margin_1year_amt,
	nvl(t.margin_1year_cnt, 0) as margin_1year_cnt,
	nvl(t.art_1year_amt, 0) as art_1year_amt,
	nvl(t.art_1year_cnt, 0) as art_1year_cnt,
	nvl(t.installment_1year_amt, 0) as installment_1year_amt,
	nvl(t.installment_1year_cnt, 0) as installment_1year_cnt,
	nvl(t.installment2_1year_amt, 0) as installment2_1year_amt,
	nvl(t.installment2_1year_cnt, 0) as installment2_1year_cnt,
	nvl(t.installment6_1year_amt, 0) as installment6_1year_amt,
	nvl(t.installment6_1year_cnt, 0) as installment6_1year_cnt,

	nvl(five.order_5year_amt, 0) as order_5year_amt,
	nvl(five.order_5year_cnt, 0) as order_5year_cnt,
	nvl(five.goods_mean_5year, 0) as goods_mean_5year,
	nvl(five.goods_max_5year, 0) as goods_max_5year,
	nvl(five.valid_5year_cnt, 0) as valid_5year_cnt,
	nvl(five.invalid_5year_cnt, 0) as invalid_5year_cnt,
	nvl(five.cancel_5year_cnt, 0) as cancel_5year_cnt,
	nvl(five.offer_5year_amt, 0) as offer_5year_amt,
	nvl(five.train_5year_amt, 0) as train_5year_amt,
	nvl(five.train_5year_cnt, 0) as train_5year_cnt,
	nvl(five.train_insure_5year_amt, 0) as train_insure_5year_amt,
	nvl(five.train_insure_5year_cnt, 0) as train_insure_5year_cnt,
	nvl(five.domestic_flight_5year_amt, 0) as domestic_flight_5year_amt,
	nvl(five.domestic_flight_5year_cnt, 0) as domestic_flight_5year_cnt,
	nvl(five.internation_flight_insure_5year_amt, 0) as internation_flight_insure_5year_amt,
	nvl(five.internation_flight_5year_cnt, 0) as internation_flight_5year_cnt,
	nvl(five.return_flight_5year_amt, 0) as return_flight_5year_amt,
	nvl(five.return_flight_5year_cnt, 0) as return_flight_5year_cnt,
	nvl(five.flight_5year_amt, 0) as flight_5year_amt,
	nvl(five.flight_5year_cnt, 0) as flight_5year_cnt,
	nvl(five.service_flight_5year_cnt, 0) as service_flight_5year_cnt,
	nvl(five.wine_5year_amt, 0) as wine_5year_amt,
	nvl(five.wine_5year_cnt, 0) as wine_5year_cnt,
	nvl(five.wine_5year_max_price, 0) as wine_5year_max_price,
	nvl(five.wine_5year_mean_price, 0) as wine_5year_mean_price,
	nvl(five.high_wine_5year_amt, 0) as high_wine_5year_amt,
	nvl(five.high_wine_5year_cnt, 0) as high_wine_5year_cnt,
	nvl(five.high_wine_5year_max_price, 0) as high_wine_5year_max_price,
	nvl(five.high_wine_5year_mean_price, 0) as high_wine_5year_mean_price,
	nvl(five.jel_5year_amt, 0) as jel_5year_amt,
	nvl(five.jel_5year_cnt, 0) as jel_5year_cnt,
	nvl(five.jel_5year_max_price, 0) as jel_5year_max_price,
	nvl(five.jel_5year_mean_price, 0) as jel_5year_mean_price,
	nvl(five.lg_5year_amt, 0) as lg_5year_amt,
	nvl(five.lg_5year_cnt, 0) as lg_5year_cnt,
	nvl(five.lg_5year_max_price, 0) as lg_5year_max_price,
	nvl(five.lg_5year_mean_price, 0) as lg_5year_mean_price,
	nvl(five.tel_5year_amt, 0) as tel_5year_amt,
	nvl(five.tel_5year_cnt, 0) as tel_5year_cnt,
	nvl(five.tel_100_5year_cnt, 0) as tel_100_5year_cnt,
	nvl(five.tel_200_5year_cnt, 0) as tel_200_5year_cnt,
	nvl(five.tel_5year_max_price, 0) as tel_5year_max_price,
	nvl(five.tel_5year_mean_price, 0) as tel_5year_mean_price,
	nvl(five.jdcard_5year_cnt, 0) as jdcard_5year_cnt,
	nvl(five.jdcard_5year_amt, 0) as jdcard_5year_amt,
	nvl(five.auction_5year_amt, 0) as auction_5year_amt,
	nvl(five.auction_5year_cnt, 0) as auction_5year_cnt,
	nvl(five.margin_5year_amt, 0) as margin_5year_amt,
	nvl(five.margin_5year_cnt, 0) as margin_5year_cnt,
	nvl(five.art_5year_amt, 0) as art_5year_amt,
	nvl(five.art_5year_cnt, 0) as art_5year_cnt,
	nvl(five.installment_5year_amt, 0) as installment_5year_amt,
	nvl(five.installment_5year_cnt, 0) as installment_5year_cnt,
	nvl(five.installment2_5year_amt, 0) as installment2_5year_amt,
	nvl(five.installment2_5year_cnt, 0) as installment2_5year_cnt,
	nvl(five.installment6_5year_amt, 0) as installment6_5year_amt,
	nvl(five.installment6_5year_cnt, 0) as installment6_5year_cnt,
	nvl(lev.avg_level,0) AS avg_level,
	nvl(lev.all_max_level,0) AS all_max_level,
	nvl(lev.high_level_amount,0) AS high_level_amount,
	nvl(lev.high_level_count,0) AS high_level_count,
	nvl(lev.watch_max_level,0) AS watch_max_level,
	nvl(lev.watch_avg_level,0) AS watch_avg_level,
	nvl(lev.clo_max_level,0) AS clo_max_level,
	nvl(lev.clo_avg_level,0) AS clo_avg_level,
	nvl(lev.wine_max_level,0) AS wine_max_level,
	nvl(lev.wine_avg_level,0) AS wine_avg_level,
	nvl(lev.jw_max_level,0) AS jw_max_level,
	nvl(lev.jw_avg_level,0) AS jw_avg_level,
	nvl(lev.ele_max_level,0) AS ele_max_level,
	nvl(lev.ele_avg_level,0) AS ele_avg_level,
	nvl(lev.mobile_max_level,0) AS mobile_max_level,
	nvl(lev.mobile_avg_level,0) AS mobile_avg_level,
	nvl(lev.milkpow_max,0) AS milkpow_max_level,
	nvl(lev.milkpow_avg,0) AS milkpow_avg_level,
	nvl(lev.diaper_max,0) AS diaper_max_level,
	nvl(lev.diaper_avg,0) AS diaper_avg_level
from tmp.zqp_userinfo_order_1year_feature t
left join tmp.zqp_order_lastyear_pin_5year_feature five
on t.acct=five.pin
LEFT JOIN tmp.goods_level_feature_v1 lev 
ON lower(trim(t.acct))=lower(trim(lev.USER))
;

-- (8) 浏览、搜索汇总指标
drop table if exists tmp.djjf_visit_summry;
create table tmp.djjf_visit_summry
as
select 
	lower(trim(user_log_acct)) as acct,
	datediff('2018-12-13', max(dt)) as last_visit_days,
	
	--pv
	sum(case when dt>=date_add('2018-12-13', -365)
			 then pv
			 else 0
		end) as pv_1year,
	sum(case when dt>=date_add('2018-12-13', -180)
			 then pv
			 else 0
		end) as pv_6mon,
	sum(case when dt>=date_add('2018-12-13', -90)
			 then pv
			 else 0
		end) as pv_3mon,
	sum(case when dt>=date_add('2018-12-13', -30)
			 then pv
			 else 0
		end) as pv_1mon,
	sum(case when dt>=date_add('2018-12-13', -7)
			 then pv
			 else 0
		end) as pv_7day,
	sum(case when dt>=date_add('2018-12-13', -3)
			 then pv
			 else 0
		end) as pv_3day,
	sum(case when dt=date_add('2018-12-13', -1)
			 then pv
			 else 0
		end) as pv_1day,
		
	--click_times
	sum(case when dt>=date_add('2018-12-13', -365)
			 then click_times
			 else 0
		end) as click_cnt_1year,
	sum(case when dt>=date_add('2018-12-13', -180)
			 then click_times
			 else 0
		end) as click_cnt_6mon,
	sum(case when dt>=date_add('2018-12-13', -90)
			 then click_times
			 else 0
		end) as click_cnt_3mon,
	sum(case when dt>=date_add('2018-12-13', -30)
			 then click_times
			 else 0
		end) as click_cnt_1mon,
	sum(case when dt>=date_add('2018-12-13', -7)
			 then click_times
			 else 0
		end) as click_cnt_7day,
	sum(case when dt>=date_add('2018-12-13', -3)
			 then click_times
			 else 0
		end) as click_cnt_3day,
	sum(case when dt=date_add('2018-12-13', -1)
			 then click_times
			 else 0
		end) as click_cnt_1day,
		
	--search_times
	sum(case when dt>=date_add('2018-12-13', -365)
			 then search_times
			 else 0
		end) as search_cnt_1year,
	sum(case when dt>=date_add('2018-12-13', -180)
			 then search_times
			 else 0
		end) as search_cnt_6mon,
	sum(case when dt>=date_add('2018-12-13', -90)
			 then search_times
			 else 0
		end) as search_cnt_3mon,
	sum(case when dt>=date_add('2018-12-13', -30)
			 then search_times
			 else 0
		end) as search_cnt_1mon,
	sum(case when dt>=date_add('2018-12-13', -7)
			 then search_times
			 else 0
		end) as search_cnt_7day,
	sum(case when dt>=date_add('2018-12-13', -3)
			 then search_times
			 else 0
		end) as search_cnt_3day,
	sum(case when dt=date_add('2018-12-13', -1)
			 then search_times
			 else 0
		end) as search_cnt_1day,
		
	--visits
	sum(case when dt>=date_add('2018-12-13', -365)
			 then visits
			 else 0
		end) as visit_cnt_1year,
	sum(case when dt>=date_add('2018-12-13', -180)
			 then visits
			 else 0
		end) as visit_cnt_6mon,
	sum(case when dt>=date_add('2018-12-13', -90)
			 then visits
			 else 0
		end) as visit_cnt_3mon,
	sum(case when dt>=date_add('2018-12-13', -30)
			 then visits
			 else 0
		end) as visit_cnt_1mon,
	sum(case when dt>=date_add('2018-12-13', -7)
			 then visits
			 else 0
		end) as visit_cnt_7day,
	sum(case when dt>=date_add('2018-12-13', -3)
			 then visits
			 else 0
		end) as visit_cnt_3day,
	sum(case when dt=date_add('2018-12-13', -1)
			 then visits
			 else 0
		end) as visit_cnt_1day,
		
	--item_page_detail，查看商详页个数
	sum(case when dt>=date_add('2018-12-13', -365)
			 then item_page_detail
			 else 0
		end) as item_page_detail_cnt_1year,
	sum(case when dt>=date_add('2018-12-13', -180)
			 then item_page_detail
			 else 0
		end) as item_page_detail_cnt_6mon,
	sum(case when dt>=date_add('2018-12-13', -90)
			 then item_page_detail
			 else 0
		end) as item_page_detail_cnt_3mon,
	sum(case when dt>=date_add('2018-12-13', -30)
			 then item_page_detail
			 else 0
		end) as item_page_detail_cnt_1mon,
	sum(case when dt>=date_add('2018-12-13', -7)
			 then item_page_detail
			 else 0
		end) as item_page_detail_cnt_7day,
	sum(case when dt>=date_add('2018-12-13', -3)
			 then item_page_detail
			 else 0
		end) as item_page_detail_cnt_3day,
	sum(case when dt=date_add('2018-12-13', -1)
			 then item_page_detail
			 else 0
		end) as item_page_detail_cnt_1day,
		
	--sku
	sum(case when dt>=date_add('2018-12-13', -365)
			 then item_detail
			 else 0
		end) as sku_cnt_1year,
	sum(case when dt>=date_add('2018-12-13', -180)
			 then item_detail
			 else 0
		end) as sku_cnt_6mon,
	sum(case when dt>=date_add('2018-12-13', -90)
			 then item_detail
			 else 0
		end) as sku_cnt_3mon,
	sum(case when dt>=date_add('2018-12-13', -30)
			 then item_detail
			 else 0
		end) as sku_cnt_1mon,
	sum(case when dt>=date_add('2018-12-13', -7)
			 then item_detail
			 else 0
		end) as sku_cnt_7day,
	sum(case when dt>=date_add('2018-12-13', -3)
			 then item_detail
			 else 0
		end) as sku_cnt_3day,
	sum(case when dt=date_add('2018-12-13', -1)
			 then item_detail
			 else 0
		end) as sku_cnt_1day,
		
	--加购数
	sum(case when dt>=date_add('2018-12-13', -365)
			 then cart_num
			 else 0
		end) as cart_cnt_1year,
	sum(case when dt>=date_add('2018-12-13', -180)
			 then cart_num
			 else 0
		end) as cart_cnt_6mon,
	sum(case when dt>=date_add('2018-12-13', -90)
			 then cart_num
			 else 0
		end) as cart_cnt_3mon,
	sum(case when dt>=date_add('2018-12-13', -30)
			 then cart_num
			 else 0
		end) as cart_cnt_1mon,
	sum(case when dt>=date_add('2018-12-13', -7)
			 then cart_num
			 else 0
		end) as cart_cnt_7day,
	sum(case when dt>=date_add('2018-12-13', -3)
			 then cart_num
			 else 0
		end) as cart_cnt_3day,
	sum(case when dt=date_add('2018-12-13', -1)
			 then cart_num
			 else 0
		end) as cart_cnt_1day,
		
	--总访问时长
	sum(case when dt>=date_add('2018-12-13', -365)
			 then all_rt
			 else 0
		end) as visit_dur_1year,
	sum(case when dt>=date_add('2018-12-13', -180)
			 then all_rt
			 else 0
		end) as visit_dur_6mon,
	sum(case when dt>=date_add('2018-12-13', -90)
			 then all_rt
			 else 0
		end) as visit_durt_3mon,
	sum(case when dt>=date_add('2018-12-13', -30)
			 then all_rt
			 else 0
		end) as visit_dur_1mon,
	sum(case when dt>=date_add('2018-12-13', -7)
			 then all_rt
			 else 0
		end) as visit_dur_7day,
	sum(case when dt>=date_add('2018-12-13', -3)
			 then all_rt
			 else 0
		end) as visit_dur_3day,
	sum(case when dt=date_add('2018-12-13', -1)
			 then all_rt
			 else 0
		end) as visit_dur_1day
	
from adm.adm_s14_ol_user_di
where dt >= date_add('2018-12-13', -365) and dt <= date_add('2018-12-13', -1) and length(lower(trim(user_log_acct)))>0
group by lower(trim(user_log_acct))
;

-- (9) 所有特质汇总
drop table if exists tmp.zqp_userinfo_order_1and5year_feature;
create table tmp.zqp_userinfo_order_1and5year_feature
as
select 
	t.*,
	nvl(visit.last_visit_days, 365) as last_visit_days,
	nvl(visit.pv_1year,0) as pv_1year,
	nvl(visit.pv_6mon,0) as pv_6mon,
	nvl(visit.pv_3mon,0) as pv_3mon,
	nvl(visit.pv_1mon,0) as pv_1mon,
	nvl(visit.pv_7day,0) as pv_7day,
	nvl(visit.pv_3day,0) as pv_3day,
	nvl(visit.pv_1day,0) as pv_1day,
	nvl(visit.click_cnt_1year,0) as click_cnt_1year,
	nvl(visit.click_cnt_6mon,0) as click_cnt_6mon,
	nvl(visit.click_cnt_3mon,0) as click_cnt_3mon,
	nvl(visit.click_cnt_1mon,0) as click_cnt_1mon,
	nvl(visit.click_cnt_7day,0) as click_cnt_7day,
	nvl(visit.click_cnt_3day,0) as click_cnt_3day,
	nvl(visit.click_cnt_1day,0) as click_cnt_1day,
	nvl(visit.search_cnt_1year,0) as search_cnt_1year,
	nvl(visit.search_cnt_6mon,0) as search_cnt_6mon,
	nvl(visit.search_cnt_3mon,0) as search_cnt_3mon,
	nvl(visit.search_cnt_1mon,0) as search_cnt_1mon,
	nvl(visit.search_cnt_7day,0) as search_cnt_7day,
	nvl(visit.search_cnt_3day,0) as search_cnt_3day,
	nvl(visit.search_cnt_1day,0) as search_cnt_1day,
	nvl(visit.visit_cnt_1year,0) as visit_cnt_1year,
	nvl(visit.visit_cnt_6mon,0) as visit_cnt_6mon,
	nvl(visit.visit_cnt_3mon,0) as visit_cnt_3mon,
	nvl(visit.visit_cnt_1mon,0) as visit_cnt_1mon,
	nvl(visit.visit_cnt_7day,0) as visit_cnt_7day,
	nvl(visit.visit_cnt_3day,0) as visit_cnt_3day,
	nvl(visit.visit_cnt_1day,0) as visit_cnt_1day,
	nvl(visit.item_page_detail_cnt_1year,0) as item_page_detail_cnt_1year,
	nvl(visit.item_page_detail_cnt_6mon,0) as item_page_detail_cnt_6mon,
	nvl(visit.item_page_detail_cnt_3mon,0) as item_page_detail_cnt_3mon,
	nvl(visit.item_page_detail_cnt_1mon,0) as item_page_detail_cnt_1mon,
	nvl(visit.item_page_detail_cnt_7day,0) as item_page_detail_cnt_7day,
	nvl(visit.item_page_detail_cnt_3day,0) as item_page_detail_cnt_3day,
	nvl(visit.item_page_detail_cnt_1day,0) as item_page_detail_cnt_1day,
	nvl(visit.sku_cnt_1year,0) as sku_cnt_1year,
	nvl(visit.sku_cnt_6mon,0) as sku_cnt_6mon,
	nvl(visit.sku_cnt_3mon,0) as sku_cnt_3mon,
	nvl(visit.sku_cnt_1mon,0) as sku_cnt_1mon,
	nvl(visit.sku_cnt_7day,0) as sku_cnt_7day,
	nvl(visit.sku_cnt_3day,0) as sku_cnt_3day,
	nvl(visit.sku_cnt_1day,0) as sku_cnt_1day,
	nvl(visit.cart_cnt_1year,0) as cart_cnt_1year,
	nvl(visit.cart_cnt_6mon,0) as cart_cnt_6mon,
	nvl(visit.cart_cnt_3mon,0) as cart_cnt_3mon,
	nvl(visit.cart_cnt_1mon,0) as cart_cnt_1mon,
	nvl(visit.cart_cnt_7day,0) as cart_cnt_7day,
	nvl(visit.cart_cnt_3day,0) as cart_cnt_3day,
	nvl(visit.cart_cnt_1day,0) as cart_cnt_1day,
	nvl(visit.visit_dur_1year,0) as visit_dur_1year,
	nvl(visit.visit_dur_6mon,0) as visit_dur_6mon,
	nvl(visit.visit_durt_3mon,0) as visit_durt_3mon,
	nvl(visit.visit_dur_1mon,0) as visit_dur_1mon,
	nvl(visit.visit_dur_7day,0) as visit_dur_7day,
	nvl(visit.visit_dur_3day,0) as visit_dur_3day,
	nvl(visit.visit_dur_1day,0) as visit_dur_1day

from tmp.zqp_userinfo_order_1and5year_feature_0 t
left join tmp.djjf_visit_summry visit
on t.acct=visit.acct
;

-- 2 生成最终待预测名单

---(2.1) 得到所有的非企业样本及半年内没有购物的人    Time taken: 710.988 seconds
-- 个人用户表：tmp.gdm_m01_userinfo_personal_reg0_99_lv90_ep_ord_regsrc616 personal
-- https://cf.jd.com/pages/viewpage.action?pageId=133434523
drop table if exists tmp.djjf_sms2_pred_features;
create table tmp.djjf_sms2_pred_features
as
select 
	t.*
from tmp.zqp_userinfo_order_1and5year_feature  t   
inner join tmp.gdm_m01_userinfo_personal_reg0_99_lv90_ep_ord_regsrc616 personal
on lower(trim(personal.user_log_acct))=t.acct
where t.last_ord_days<=180 
;


-- 3 生成训练数据
drop table if exists tmp.djjf_sms2_train_yy_list;
create table tmp.djjf_sms2_train_yy_list 
as
select 
	lower(trim(jdpin)) as acct,
	substr(max(yy_time),1,10) as yydate
from app.djjf_yy_all
where length(lower(trim(jdpin)))>0
group by lower(trim(jdpin))
;


--给定抽样的随机数指标，排除预约用户
drop table if exists tmp.djjf_sms2_pred_features_label_idx;
create table tmp.djjf_sms2_pred_features_label_idx
as
select 
	t.*,
	0 as label,
	rand()*240000000 as sample_idx
from  tmp.djjf_sms2_pred_features t 
left join 
(select * from tmp.djjf_sms2_train_yy_list
where yydate not in ('2018-10-27','2018-10-28')) yy  --使用27、28两天预约匹配数据监测模型有效性
on lower(trim(t.acct)) = lower(trim(yy.acct))
where yy.acct is null
;


-- 商城负样本
drop table if exists tmp.djjf_sms2_train_data_neg_mall;
create table tmp.djjf_sms2_train_data_neg_mall
as
select 
	t.*
from  tmp.djjf_sms2_pred_features_label_idx t
left join tmp.djjf_sms2_train_yy_list yy
on lower(trim(t.acct)) = lower(trim(yy.acct))
where sample_idx>0 and sample_idx<1200000 and (yy.acct is null)
;

drop table if exists tmp.djjf_sms2_train_data_pos;
create table tmp.djjf_sms2_train_data_pos
as
select 
	t.*,
	1 as label
from tmp.djjf_sms2_pred_features t
inner join tmp.djjf_sms2_train_yy_list yy
on lower(trim(t.acct)) = lower(trim(yy.acct))
where yy.yydate not in ('2018-10-27','2018-10-28') --使用27、28两天预约匹配数据监测模型有效性
;

--hive -e "set hive.cli.print.header=true;select * from tmp.djjf_sms2_train_data_pos" > /data0/cjh/sms3_real/pos.csv
--hive -e "set hive.cli.print.header=true;select * from tmp.djjf_sms2_train_data_neg_mall" > /data0/cjh/sms3_real/neg.csv

--spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer --conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer --conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_algorithm:latest --executor-memory 16g --executor-cores 12 --conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_algorithm:latest --py-files gbdt.m richuser_recog_pred.py
