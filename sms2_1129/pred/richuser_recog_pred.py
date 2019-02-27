# -*- coding:utf-8 -*-
"""
@author:zzh
@file:hwup_oh_scal_gbt_all_cluster_20w.py
@time:2018/9/316:08
"""
#spark
from __future__ import division
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
import pyspark.sql.types as typ
import pyspark.sql.functions as fn
import pyspark.ml.feature as ft
import pyspark.ml.classification as cl
from pyspark.ml import Pipeline
import pyspark.ml.evaluation as ev
from pyspark.ml import PipelineModel
from datetime import datetime
from pyspark.sql.types import Row

#sklearn
import numpy as np
import sklearn as skl
import pandas as pd
from  sklearn.utils  import shuffle
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import  preprocessing
from sklearn.metrics import  roc_auc_score
from sklearn import metrics
import matplotlib.pyplot as plt
import matplotlib as mpl
from sklearn.model_selection import cross_val_score
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
import gc

import os
os.environ["PYSPARK_PYTHON"]="/usr/bin/python"
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
start_time = datetime.now()


#加载命令
#spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer --conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer --conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_algorithm:latest --executor-memory 16g --executor-cores 12 --conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_algorithm:latest --py-files gbdt.m richuser_recog_pred.py



#===<1> 全局变量设置 <begin>===

#是否开启模型测试
PRED_TEST_MODE = 1

#spark及hive句柄
sc = SparkContext()
sqlContext = HiveContext(sc)

#模型路径
modelPath = 'gbdt.m'

#加载模型
gbdt = joblib.load(modelPath)
print("gbdt.m load success !")
#===<1> 全局变量设置 <end>===


#===<2> 预测高净值客户 <begin>===
#测试模型有效性
if PRED_TEST_MODE == 1:
	neg = sqlContext.sql("""
	SELECT 
	*
	FROM tmp.djjf_sms2_train_data_neg_mall
	""")
	
	pos = sqlContext.sql("""
	SELECT 
	*
	FROM tmp.djjf_sms2_train_data_pos
	""")

	neg = neg.drop('sample_idx')
	df_t = neg.union(pos)
	df_t_preds = df_t.rdd.map(lambda row :Row(label=row['label'], prediction=int(gbdt.predict([row[1:-1]])[0]), prob1=float(gbdt.predict_proba([row[1:-1]])[0,1]))).toDF()
	df_t_preds = df_t_preds.toPandas()
	print("Area Under ROC: ", metrics.roc_auc_score(df_t_preds['label'], df_t_preds['prob1']))
	print("precision_score: ", metrics.precision_score(df_t_preds['label'], df_t_preds['prediction']))
	print("recall_score: ", metrics.recall_score(df_t_preds['label'], df_t_preds['prediction']))


df = sqlContext.sql("""SELECT * FROM tmp.djjf_sms2_pred_features_label_idx""")

df = df.drop('sample_idx','label')

df_pred_result = df.rdd.map(lambda row :Row(acct=row[0], prediction=int(gbdt.predict([row[1:]])[0]), prob0=float(gbdt.predict_proba([row[1:]])[0,0]), prob1=float(gbdt.predict_proba([row[1:]])[0,1]))).toDF()

print("predict result success !")

df_pred_result.createOrReplaceTempView("result_to_hive_temp_table")

sqlContext.sql("""DROP TABLE IF EXISTS tmp.djjf_richuser_pred_list_sms2_1129""")

sqlContext.sql("""
create table tmp.djjf_richuser_pred_list_sms2_1129 as
select
	acct,
	prediction,
	prob0,
	prob1
from result_to_hive_temp_table
""")

end_time = datetime.now()
print(">>>Total runtime %s." % (end_time - start_time))
#===<2> 预测高净值客户 <end>===