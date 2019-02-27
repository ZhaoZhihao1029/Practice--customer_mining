
# coding: utf-8

# ## 预约客户分类模型

# In[1]:


import sklearn as skl
import pandas as pd
import numpy as np
from  sklearn.utils  import shuffle
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import  preprocessing
from sklearn.metrics import  roc_auc_score
from sklearn import metrics
from sklearn.model_selection import cross_val_score


# In[2]:


#导出训练样本
#hive -e "set hive.cli.print.header=true;select * from tmp.djjf_sms2_train_data_pos" > /data0/ljl/omit_time/pos.csv
#hive -e "set hive.cli.print.header=true;select * from tmp.djjf_sms2_train_data_neg_mall" > /data0/ljl/omit_time/neg.csv


# ### 数据读取

# In[4]:


pos = pd.read_csv('pos.csv',encoding='utf-8',sep='\t')
neg = pd.read_csv('neg.csv',encoding='utf-8',sep='\t')
print(pos.shape,neg.shape)


# In[5]:

neg = neg.drop('sample_idx',1)


# In[6]:


print(pos.shape)
print(neg.shape)


# ###  预测数据准备

# In[7]:


df = pd.concat([neg,pos],axis=0)
df = df.drop('acct',1)


# In[8]:


for i in range(10):
    df = df.sample(frac=1)


# In[9]:


df.iloc[:,:].shape


# In[10]:


X_train,X_test,y_train,y_test = train_test_split(df.iloc[:,:-1],df.label,test_size=0.05, random_state=42)
# #  使用MinMaxScaler归一化，one hot 编码不会改变
# min_max_scaler = preprocessing.MinMaxScaler()
# X_train_minmax = min_max_scaler.fit_transform(X_train)


# ###  训练模型

# In[28]:


from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier

gbdt = GradientBoostingClassifier(n_estimators=200, max_depth=5)
gbdt.fit(X_train, y_train)
#scores = cross_val_score(gbdt, df.iloc[:,:-1], df.label, scoring='roc_auc',cv=5)
#scores

score = gbdt.feature_importances_
f_importances = []
for i in range(score.shape[0]):
    c = (df.columns[i+1],score[i])
    f_importances.append(c)

f_importances_sort = sorted(f_importances, key=lambda c:c[1], reverse=True)
for i in range(10):
    print(f_importances_sort[i])


# ###  预测

# In[ ]:


y_predict_prob = gbdt.predict_proba(X_test)
print(metrics.roc_auc_score(y_test,y_predict_prob[:,1]))


# In[ ]:


print(metrics.precision_score(y_test, gbdt.predict(X_test)))


# In[ ]:


print(metrics.recall_score(y_test, gbdt.predict(X_test)))


# In[ ]:


from sklearn.externals import joblib
joblib.dump(gbdt, 'gbdt.m')

