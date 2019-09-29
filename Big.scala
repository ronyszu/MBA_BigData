scala> import spark.implicits._
scala> import spark.sql
scala> import org.apache.spark.sql._



object Big {


  def showWarning() {
    System.err.println(
      """ TRABALHO BIG DATA - MBA ENGENHARIA DE SOFTWARE 2019.1
      PROFESSOR ALEXANDRE ASSIS

      """.stripMargin)
  }

  def main(args: Array[String]) {

    showWarning()
    val df = spark.read.format("csv").option("sep",",").option("header","true").load("sample.csv")
    df.printSchema

    df.write.mode(SaveMode.Overwrite).saveAsTable("dfTable")

    val dfDomain = spark.sql("SELECT domain,count(*) from dfTable where domain is not null group by domain order by count(*) desc")

    val dfType = spark.sql("SELECT type,count(*) from dfTable where type is not null group by type order by count(*) desc")

    dfDomain.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("domain.csv")
    dfType.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("type.csv")

    println(s"Final!")
  }
}
