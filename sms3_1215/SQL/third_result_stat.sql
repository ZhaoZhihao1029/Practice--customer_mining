
select
       count(distinct t.pin) as cnt,
       sum(case when yy.acct is not null
                then 1
                else 0
          end) as yy_cnt,
       sum(case when (yy.acct is not null) and (channel= 'jdjrsmsai' or channel='h5_jdjrsmsai')
                then 1
                else 0
          end) as yy_channel_cnt
from dwd.dwd_mkt_uep_sms_send_detail_i_d t
inner join dev_tmp.cjh_1217yuyue yy
on lower(trim(t.pin))=lower(trim(yy.acct))
where dt='2018-12-15' and task_id='12488'
;

--7天活跃统计
select
       count(distinct t.pin) as cnt,
       sum(case when yy.acct is not null
                then 1
                else 0
          end) as yy_cnt,
       sum(case when (yy.acct is not null) and (channel= 'jdjrsmsai' or channel='h5_jdjrsmsai')
                then 1
                else 0
          end) as yy_channel_cnt
from dwd.dwd_mkt_uep_sms_send_detail_i_d t
inner join dev_tmp.cjh_1215_7active lu
on lower(trim(t.pin))=lower(trim(lu.acct))
left join dev_tmp.cjh_1217yuyue yy
on lower(trim(t.pin))=lower(trim(yy.acct))
where dt='2018-12-15' and task_id='12488'
;

--重复发送
select
       count(t.pin) as cnt,
       sum(case when yy.acct is not null
                then 1
                else 0
          end) as yy_cnt,
          sum(case when (yy.acct is not null) and (channel= 'jdjrsmsai' or channel='h5_jdjrsmsai')
                then 1
                else 0
          end) as yy_channel_cnt
from (select lower(trim(pin)) as pin
     from dwd.dwd_mkt_uep_sms_send_detail_i_d
     where dt='2018-11-29' AND task_id=11429) t
inner join (select lower(trim(pin)) as pin
     from dwd.dwd_mkt_uep_sms_send_detail_i_d
     where dt='2018-12-15' AND task_id=12488) b
on t.pin = b.pin
left join dev_tmp.cjh_1217yuyue yy
on lower(trim(t.pin))=lower(trim(yy.acct))
;

--发送日期 taskid发送成功人数 预约人数
--11.06 10148 116840 404
--11.29 11429 206172 1151
--12.15 12488 322107 2560
select
	count(*) as cnt,
	sum(case when yy.acct is not null
			 then 1
			 else 0
		end) as yy_cnt
from
(
select lower(trim(pin)) as pin
from dwd.dwd_mkt_uep_sms_send_detail_i_d 
where dt='2018-11-06' and task_id=10148 and status=1
) t
left join
(select * from dev_tmp.djjf_yy_all
where to_date(yytime)>='2018-11-06' and to_date(yytime)<='2018-11-08'
) yy
on lower(trim(t.pin))=lower(trim(yy.acct))
;
/*
还有待验证的为本次投放的效果究竟如何，
不同活动的重复投放效果如何，同种活动重复投放的效果又如何
新标签是否有扩大的必要
*/

