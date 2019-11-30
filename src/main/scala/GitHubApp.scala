import org.apache.spark.sql.SparkSession

object GitHubApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("GitHubApp").getOrCreate
    val ghLog = spark.read.json("/user/student/data/github/2017-11-20-0.json")
    val pushes = ghLog.filter("type = 'PushEvent'")

    println("schema: ")
    pushes.printSchema

    println("all events: " + ghLog.count)
    println("only pushes: " + pushes.count)

    println("first 5 rows: ")
    pushes.show(5)

    println("first 5 rows grouped by actor.login: ")
    val grouped = pushes.groupBy("actor.login").count
    grouped.show(5)

    println("first 5 rows grouped by actor.login and ordered: ")
    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)

    spark.stop()
  }
}
