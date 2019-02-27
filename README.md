# Practice--customer_mining

项目背景：运用AI模型和大数据分析商城数据和商城相关外围生态链数据，挖掘有理财倾向但未在京东金融注册的高净值客户。

任务模块：
数据收集（金服数据、商城数据、商城敏感数据、房多多数据、金融数据）；
大数据环境测试（hive读写、spark模型）；
数据分析（商城及金服高消费人群分析、金服高净值客户挖掘分析报告、电话号码传播分析、金融标签分析等）；
特征发现（基本属性、消费特征、高档品牌及品类、奢侈品、浏览特征等）；
模型开发（过滤企业用户，高净值用户分类模型）

技术要求：Hive、Spark（Python和Scala）

主要工作：
	技术准备：
		学习hive及spark
	数据收集：
		金服数据（成交、预约、到站数据）；
		商城数据（流量表、用户表、订单表）；
		商城敏感数据（敏感数据表申请、解密及通过udf查询解密数据）；
		房多多数据（地址拆分【未完】）；
		金融数据（猛犸系统数据申请、上传及查询结果列转行）
	大数据环境测试：
		hive读取hql并通过spark训练、评估及预测demo；
		spark-submit提交作业调优
	数据分析：
		商城及金服高消费人群分析（购物金额分布、注册类型及用户级别占比等）；
		金服高净值客户挖掘分析报告（主要针对已申请的金融数据标签和模型特征重要性高的特征进行分析，金融数据标签包括日活跃用户查全率和查准率、不同时间窗口下的重复率和新用户率、活跃周数、大理财历史最高持仓、标签组合分析，模型特征中的5年分期金额特征分析）；
		电话号码传播分析（由金服成交用户提供的电话号关联敏感表基本用户信息表查询京东pin，对匹配到的京东pin关联查询订单表匹配订单电话号，执行两次传播）；
		金融标签分析（单标签：分析7日活跃用户、30天浏览金融app、30天浏览金融web、大理财持仓40w+、一年5单保险订单且总金额大于10w、30天点击大理财埋点、30天点击股票财迷埋点及标签组合；自定义标签：铂金会员邀请激活表、基金定投预约表、火意险保单表、航意险交易请求表、基金交易单、保险理财交易单、小金库交易单；复现标签：近30天登陆金融app ip地址解析为北京的用户、近3个月点击东家财富类活动、近30天浏览过东家财富页面、用户可投资资产50w+且近15天登陆过金融app、近15天登陆金融app、大理财历史最高持仓段为50w+且近15天登陆过金融app、用户当前持仓为金主、准百万富翁、百万富翁、千万富翁且近15天登陆过金融APP、小金库上年及当前度贡献度250+且近7天登陆过金融app、大理财上年及当前度贡献度250+且近7天登陆过金融app、私募业务风险测评结果为保守、稳健、平衡、成长、进取、用户可投资资产50w且近30天搜索过理财、近30天搜索理财用户）；
	模型开发：
		过滤企业用户（依据5个逻辑过滤条件区分个人用户和企业用户：用户注册类型、用户级别、企业用户表、关联企销订单、用户注册来源明细，并封装成定时任务更新）；
		高净值用户分类模型（将python代码改写成spark，并分析特征重要性等）


投放结果：
投放后三天内统计结果：
投放轮次	投放时间	  投放量	  发送成功量	  短信点击量	  短信点击率	  预约转化量	  预约转化率
1	   2018.11.06	200000	   116813	8665	     7.42%	277         3.20%
2	   2018.11.29	440856	   206097	10067	     4.88%	657         6.53%
3	   2018.12.15	825623	   322060	21865	     6.79%	1961	    8.97%
