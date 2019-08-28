package com.Tags


import com.typesafe.config.ConfigFactory
import com.utils.TagsUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 上下文标签
  */
object TagsContext {
   def main(args: Array[String]): Unit = {

      if (args.length != 5) {
         println("目录不匹配，退出程序")
         sys.exit()
      }

      val Array(inputPath,outputPath,dirPath,stopPath,days) = args

      val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      val sc: SparkContext = new SparkContext(conf)
      val spark = SparkSession.builder().config(conf).getOrCreate()

      //      val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
            val df: DataFrame = spark.read.parquet(inputPath)
//      df.createOrReplaceTempView("")

      //todo 调用HBase API

      //加载配置文件
      val load = ConfigFactory.load()
      val hbaseTableName = load.getString("hbase.TableName")

      //创建Hadoop任务
      val configuration = sc.hadoopConfiguration
      configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))

      //创建HbaseConnection
      val hbconn = ConnectionFactory.createConnection(configuration)
      val hbadmin = hbconn.getAdmin
      //判断表是否可用
      if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
         //创建表操作
         val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
         val descriptor = new HColumnDescriptor("tags")
         tableDescriptor.addFamily(descriptor)
         hbadmin.createTable(tableDescriptor)
         hbadmin.close()
         hbconn.close()

      }
      //创建JobConf
      val jobconf = new JobConf(configuration)

      //指定输出类型,上边界
      jobconf.setOutputFormat(classOf[TableOutputFormat])
      jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)
      //读取字段文件
      val map = sc.textFile("dir/app_dict.txt").map(_.split("\t",-1))
            .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()

      //将处理好的数据广播
      val broadcast = sc.broadcast(map)
      //获取停用的词库
      val stopword = sc.textFile(stopPath).map((_,0)).collectAsMap()
      val bcstopword = sc.broadcast(stopword)
      //过滤
      //dataframe -> rdd 要隐式转换
      import spark.implicits._
      df.filter(TagsUtils.OneUserId)
         .map(row => {
            val userId = TagsUtils.getOneUserId(row)
            //通过row数据打上所有标签
            //广告位类型标签
            val adList: List[(String, Int)] = TagsAd.makeTags(row)
            val keywordList: List[(String, Int)] = TagsKeyWord.makeTags(row,bcstopword)
            val apfpList: List[(String, Int)] = TagsAdPlatformProvider.makeTags(row)
            val appList: List[(String, Int)] = TagsApp.makeTags(row,broadcast)
            val locList: List[(String, Int)] = TagsLoc.makeTags(row)
            val terminalList: List[(String, Int)] = TagsTerminal.makeTags(row)
            val business = BusinessTag.makeTags(row)
            (userId,adList++keywordList++apfpList++appList++locList++terminalList++business)
         }).rdd
         .reduceByKey((list1,list2) =>
            (list1:::list2)
               .groupBy(_._1)
               .mapValues(_.foldLeft[Int](0)(_+_._2))
            .toList
         )
         //偏函数,模式匹配的一种
         .map{
         case(userid,userTag)=>{

            val put: Put = new Put(Bytes.toBytes(userid))
            //处理下标签
            val tags: String = userTag.map(t=>t._1+","+t._2).mkString(",")
            put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))
            (new ImmutableBytesWritable(),put)
         }
      }
      //保存到对应表中
         .saveAsTextFile(jobconf)

   }
}
