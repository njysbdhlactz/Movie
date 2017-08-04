软件名称：基于大数据的电影推荐网站   
程序语言：Java
环境配置：jdk7.0+tomcat7.0   
时间：2017-04-06   
描述：使用hbase和mysql作为网站数据库，使用flume来监听项目输出的activity.out日志信息，不断地把增量数据自动上传到HDFS中，使用hive来创建外部表来把Flume传过来的数据进行入库，使用HQL语法来对所得数据进行处理。比如计算出平均评分最高的前二十个电影，浏览量最多的前三十个电影等等。使用协同过滤算法实现喜好推荐：用户在对某电影评分时在MYSQL的评分表中插入一条数据，以此来收集用户评分信息（MySQL），每过一段时间就对该时段内的评分数据进行协同过滤算法的MapReduce计算，计算结果是存储在HDFS里的，所以要使用sqoop工具来对HDFS中非关系型数据转发到MYSQL这样的关系型数据库中，导入到MYSQL的推荐表中。使用该表中的数据来对用户进行电影推荐。每过一段时间就清空推荐表，再次计算上一时段的评分信息。整体项目示意图如下图示


![image](https://github.com/RockRex/Pictures/blob/master/note/movieWeb.png?raw=true)