# 如何执行代码：
1. 打开AWS console，在EC2中找到key pair， 下载并修改permission ‘chmod 400 /path/to/your-key-pair.pem’

2. 在Terminal中连接EC2 ‘ssh -i /path/to/your-key-pair.pem ec2-user@your-instance-public-dns’

3. 将所要执行的python代码通过scp上传到EC2 ‘scp -i [user@]SRC_HOST:]file1 [user@]DEST_HOST:]file2’

4. 确认EC2含有 IAM Role ‘write-to-s3’

5. 将input文件分成小份

6. 在想要保存结果的dir中执行python代码 ‘python3 script_to_run.py’


# 目标：

下载客户所提供的网址中的学术文章，并上传至云。

# 要求：

1. 以pdf形式下载所有客户提供的csv文件中含有的学术文章网址（约700万个）。基础要求需完成70%，进阶要求需完成90%。

2. 将pdf形式的文章压缩成gz形式。

3. 将转换好的文件上传至客户指定的云（s3）。

4. 项目的截止日期为一个月之内（至2024/05/06）。

# 设计：

## 代码端：

1. 将总的csv文件拆解成多个小的文件，并用multi processing多线程处理各个文件

2. 每个process中，用python处理csv文件，提出所有的url，并依次下载文章。

3. 当单个process中的文章数量达到一定数量时，将全部文章压缩成gz形式并上传到s3并删除本地的数据。

4. 若执行过程中遇到status code非200或者exception，不终断执行，将url和错误信息记录下来，以便再次处理。


## 云端：

经试验，平均一篇文章约2MB，7百万篇文章共计14TB，压缩并写入s3需10TB， 共计约24TB。当带宽为100Mbps时，需308小时完成全部下载。基于此，需要云服务器帮助完成任务。
鉴于s3为客户偏爱的云存储，并且亚马逊云计算的价格合理，AWS EC2为目前的选择。

为减少s3的成本，应减少写入次数，解决方案为在ec2本地下载一定数量文章后压缩并一同上传到s3，需要较高的Storage(>64G)和Network performance(> 5Gbps)。推荐ec2 r型号，更偏重于Memory-intensive需求。

# 经济与时间成本估算

## 时间成本
1. 网络下载时间：假设网络带宽为10Gbps，24TB资源下载时间约为24000/1.25 = 19200s = 5.33 h。若带宽不为10Gbps则依比例换算。
2. 响应时间：包括http响应和其他非资源下载时间的总和。假设平均响应时间为0.5s，线程为100，则共需时间7000000/100*0.5 = 35000s = 9.72h。时间随线程数按比例换算。
假设线程数为100，每个线程每一百篇文章上传一次s3，每篇文章需留有5MB storage，则共需5*100*100 = 50000MB，即约为50GB的storage。

## 经济成本
### ec2
以r5.2xlarge为参考,r5.2xlarge提供8 CPU，64GB Memory和最高可至10Gbps的带宽。可以满足任务需求。
所需花费为0.504USD每小时，即每天约12USD，一个月共需360USD,约2600CNY。若可在15天内完成任务，则成本降低至1300CNY.
其他可供参考的型号有：r5.4xlarge,c5.4xlarge,m5.4xlarge

### s3:
以下计算来自aws pricing calculator
假设选择S3 Standard - Infrequent Access

Unit conversions
S3 Standard-IA storage: 10 TB per month x 1024 GB in a TB = 10240 GB per month
S3 Standard-IA Average Object Size: 150 MB x 0.0009765625 GB in a MB = 0.146484375 GB

Pricing calculations
10,240 GB per month / 0.146484375 GB average item size = 69,905.0667 unrounded number of objects
Round up by 1 (69905.0667) = 69906 number of objects
10,240 S3 Standard-IA Storage x 0.0125 USD = 128.00 USD (S3 Standard-IA storage cost)
70,000 PUT requests for S3 Standard-IA Storage x 0.00001 USD per request = 0.70 USD (S3 Standard-IA PUT requests cost)
128.00 USD + 0.70 USD = 128.70 USD (Total S3 Standard-IA Storage and other costs)
S3 Standard - Infrequent Access (S3 Standard-IA) cost (monthly): 128.70 USD

0.146484375 S3 Standard-IA Average Object Size / 8 object size / 0.0009765625 GB = 18.75 parts (unrounded)
Round up by 1 (18.750000) = 19 parts
69,906 number of objects x 19 parts x 0.00001 USD = 13.28214 USD (Cost for PUT, COPY, POST requests for initial data)
S3 Standard - Infrequent Access (S3 Standard-IA) cost (upfront): 13.28 USD

(可以看出put request的数量几乎不会影响，因此更好的文件压缩率比少的PUT request更能节省成本)

# 问题与解决方案：

1. IP限流

2. Http Status Code 403

3. Read Timeout

4. Http Status Code 400/404/406/410

5. 上传s3失败

6. ec2空间溢出

# 工作量估算
阐明需求与调研 （1 天）
选取与购买云服务 （0.5天）
完成代码与文档（1天）
部署代码至云并运行（0.5天）
检查运行进度，调查失败请求原因（0.5天）
修改代码，重新处理失败的请求（3天）
完成客户反馈问题（1天）