### 定义一个sql limit spark extension
#### 说明
1. 用户写的spark sql查询，不带limit,数据量大的时候，会造成driver端oom，这个扩展的执行计划，会对所有没有加limit的查询，强制加上limit
,同时提供一个参数spark.sql.force.limit.rows 可以设定limit的条数，默认值是1000. 设置为-1表示禁用这个功能
2. 本例子仅供参考，上生产务必自己看代码，理解源码，看是否适配自己的场景，测试后再上生产。
#### 使用
* emr-7.1.0 spark 3.5.0+
```shell
# 编译号的jar包放到s3
S3_PATH=s3://pcd-01/tmp
# 编译好的jar， 如果自己编译: mvn clean package -Dscope.type=provided
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/spark-extension-limit-1.0.jar
aws s3 cp spark-extension-limit-1.0.jar $S3_PATH/spark-extension-limit-1.0.jar


spark-sql \
--jars $S3_PATH/spark-extension-limit-1.0.jar \
--conf spark.sql.extensions=com.aws.analytics.extension.ForceLimitExtension \
--conf spark.sql.force.limit.rows=1 


WITH sample_data AS (
    SELECT 1 as group_id, 'A' as user_id, 100 as value
    UNION ALL SELECT 1, 'B', 200
    UNION ALL SELECT 1, 'A', 300
    UNION ALL SELECT 2, 'C', 400
    UNION ALL SELECT 2, 'D', 500
    UNION ALL SELECT 2, 'C', 600
)
select * from sample_data;
```
![](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202412182259352.png)