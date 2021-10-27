// using scala 2.13.6
// using lib org.apache.spark::spark-sql:3.2.0

import sys.process._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.net.URL
import java.io.File

object Main extends App {
    // Prepare log file
    val logsFile = "apachelogs.txt"
    // val logsUrl = "http://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/apache_logs/apache_logs"
    // val process = (new URL(logsUrl) #> new File(logsFile)).!!

    // // Run Spark
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext

    // // Load logs file into RDD
    val logs = sc.textFile(logsFile)

    val logParser = new AccessLogParser()
    val parsed = logs.flatMap(logParser.parseRecord)

    val popularity = parsed
        .map(_.referer)
        .map(_.stripPrefix("\"").stripPrefix("http://").stripPrefix("https://").stripPrefix("www."))
        .map {
            case r if r.indexOf("/") != -1 => r.substring(0, r.indexOf("/"))
            case r => r
        }   
        .filter(r => !r.startsWith("semicomplete.com") && r != "-")
        .map(_ -> 1)
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)

    val traffic = parsed
        .map(r => (r.httpStatusCode -> AccessLogParser.parseDateField(r.dateTime).get.getHours()) -> 1)
        .reduceByKey(_ + _)
        .groupBy {
            case ((httpCode, hour), count) => httpCode
        }
        .flatMap {
                case (httpCode, lst) => lst.map {
                    case ((httpCode, hour), count) => (hour, httpCode, count)
                }
        }

    val extensionRegex = raw".*\.([\d|\w]+)$$".r

    val extensions = parsed
        .flatMap(r => AccessLogParser.parseRequestField(r.request))
        .filter(_._1 == "GET")
        .map {
            case (_, link, _) => link
        }
        .flatMap {
            case extensionRegex(ext) => Some(ext)
            case other => None
        }
        .map(_ -> 1)
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)

    val ipAddresses = parsed
        .map(_.clientIpAddress)
        .distinct

    // spark.createDataFrame(ipAddresses).toDF("ip").write.option("header", true).csv("ips.csv")

    val botKeywords = Seq("bot", "crawl", "Bot", "Crawl")
    val botsEstimation = parsed
        .map(_.userAgent)
        .filter(ua => botKeywords.exists(ua.contains))
        .count().toFloat / parsed.count()
                

    println(botsEstimation)
}
