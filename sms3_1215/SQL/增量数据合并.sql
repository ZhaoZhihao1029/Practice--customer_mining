--历史数据合并代码

--增量表创建
drop TABLE tmp.djjf_yy_1215_1216;
CREATE TABLE tmp.djjf_yy_1215_1216(jdpin string, channel string, yy_time string) ROW format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar"="\t") STORED AS TEXTFILE
;

--增量表上传
--LOAD DATA LOCAL inpath '/data0/ljl/up/1217yuyue.txt' overwrite INTO TABLE tmp.djjf_yy_1215_1216;

--全量表备份
drop table if exists tmp.djjf_yy_all_1221;
create table tmp.djjf_yy_all_1221
as
SELECT
* 
from app.djjf_yy_all
;

--全量表与增量表重合日期:'2018-12-15'


drop table if exists tmp.djjf_yy_all_before;
create table tmp.djjf_yy_all_before
as
SELECT
* 
from app.djjf_yy_all
where yy_time<'2018-12-15'
;

drop table if exists tmp.djjf_yy_all_after;
create table tmp.djjf_yy_all_after
as
SELECT
* 
from app.djjf_yy_all
where yy_time>='2018-12-15'
;

drop table if exists tmp.djjf_yy_all_after_merge;
create table tmp.djjf_yy_all_after_merge
as
SELECT
distinct jdpin,channel,yy_time
from 
(
select jdpin,channel,yy_time from tmp.djjf_yy_all_after
union all
select jdpin,channel,yy_time from tmp.djjf_yy_1215_1216
) t
;

drop table if exists app.djjf_yy_all;
create table app.djjf_yy_all
as
SELECT
jdpin,channel,yy_time
from 
(
select jdpin,channel,yy_time from tmp.djjf_yy_all_before
union all
select jdpin,channel,yy_time from tmp.djjf_yy_all_after_merge
) t
;