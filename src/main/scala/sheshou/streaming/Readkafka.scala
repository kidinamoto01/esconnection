package sheshou.streaming
import java.util.{Calendar, Properties}

import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
/**
  * Created by wushiliang on 17/5/6.
  */
object Readkafka {

  val log = Logger.getLogger("Readkafka")


  def main(args: Array[String]) {
   if (args.length < 4) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
        """.stripMargin)
      System.exit(1)
    }



    val Array(brokers, url, user, passwd) = args
    println(brokers)
    println(url)
    println(user)
    println(passwd)


    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("SaveKafkaData")
    val  sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(30))


    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "zookeeper.connect" -> "localhost:2181")


    val messgeVirus = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("reportvirus")).map(_._2)

    val messgeCve = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("reportcve")).map(_._2)

    val messgeOe = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("attachmentoe")).map(_._2)



    //report_virus
    messgeVirus.foreachRDD { x =>
      val hiveContext = new HiveContext(sc)
      // val text = sqlContext.read.json(x)
      val text = hiveContext.read.json(x)

      //get json schame
      text.printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        text.registerTempTable("messgevirus")
        text.printSchema()

        hiveContext.sql("insert into sheshou.attack_list partition(`year`,`month`,`day`,`hour`) select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level,year,month,day,hour  from messgevirus ")

        log.info("insert into hive is ")


        //insert into  attack_list@mysql
        val mysqlDF = hiveContext.sql("select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level  from messgevirus")
        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)
        val dfWriter = mysqlDF.write.mode("append").option("driver", "com.mysql.jdbc.Driver")
        dfWriter.jdbc(url, "attack_list", prop)
      }
    }

    //report_cve
    messgeCve.foreachRDD { x =>
      val hiveContext = new HiveContext(sc)
      // val text = sqlContext.read.json(x)
      val text = hiveContext.read.json(x)

      //get json schame
      text.printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        text.registerTempTable("messagecve")
        text.printSchema()

        hiveContext.sql("insert into sheshou.attack_list partition(`year`,`month`,`day`,`hour`) select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level,year,month,day,hour  from messagecve ")

        log.info("insert into hive is ")


        //insert into  attack_list@mysql
        val mysqlDF = hiveContext.sql("select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level  from messagecve")
        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)
        val dfWriter = mysqlDF.write.mode("append").option("driver", "com.mysql.jdbc.Driver")
        dfWriter.jdbc(url, "attack_list", prop)
      }
    }

    //attachment_oe
    messgeOe.foreachRDD { x =>
      val hiveContext = new HiveContext(sc)
      // val text = sqlContext.read.json(x)
      val text = hiveContext.read.json(x)

      //get json schame
      text.printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        text.registerTempTable("messageoe")
        text.printSchema()

        hiveContext.sql("insert into sheshou.attack_list partition(`year`,`month`,`day`,`hour`) select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level,year,month,day,hour  from messageoe")

        log.info("insert into hive is ")


        //insert into  attack_list@mysql
        val mysqlDF = hiveContext.sql("select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level  from messageoe")
        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)
        val dfWriter = mysqlDF.write.mode("append").option("driver", "com.mysql.jdbc.Driver")
        dfWriter.jdbc(url, "attack_list", prop)
      }
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
