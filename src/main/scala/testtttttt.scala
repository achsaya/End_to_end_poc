import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
object testtttttt {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("POC")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .getOrCreate()

    val rawDF = spark.read.option("header", true)
      .format("csv")
      .option("path", "input")
      .option("maxFilesPerTrigger", 1)
      .load()

    val deactiveDF = spark.read.option("header", true)
      .format("csv")
      .option("path", "deactivation_input")
      .option("maxFilesPerTrigger", 1)
      .load()

    val dropfieldDF = rawDF.drop("Replacement NPI", "Healthcare Provider Primary Taxonomy Switch_11", "Provider License Number State Code_11", "Healthcare Provider Taxonomy Code_15", "Other Provider Identifier Type Code_19", "Other Provider Identifier Type Code_22", "Other Provider Identifier_33",
      "Other Provider Identifier Issuer_36", "Other Provider Identifier_42", "Other Provider Identifier_40", "Other Provider Identifier Issuer_46")

    val df2 = dropfieldDF.withColumn("Provider Gender Code", when(col("Provider Gender Code") === "M", "M,Male")
      .when(col("Provider Gender Code") === "F", "F,Female")
      .otherwise("unknown"))

    val df3 = df2.withColumn("Entity Type Code", when(col("Entity Type Code") === "1", "1, Individual")
      .when(col("Entity Type Code") === "2", "2, Organization"))

    val df4 = df3.withColumn("Is Sole Proprietor", when(col("Is Sole Proprietor") === "X", "X, Not Answered")
      .when(col("Is Sole Proprietor") === "Y", "Y, Yes")
      .when(col("Is Sole Proprietor") === "N", "N, No"))

    val df5 = df4.withColumn("Is Organization Subpart", when(col("Is Organization Subpart") === "X", "X, Not Answered")
      .when(col("Is Organization Subpart") === "Y", "Y, Yes")
      .when(col("Is Organization Subpart") === "N", "N, No"))

    val df6 = df5.withColumn("Provider Other Organization Name Type Code", when(col("Provider Other Organization Name Type Code") === "1", "1, Former Name, I")
      .when(col("Provider Other Organization Name Type Code") === "2", "2, Professional Name, I")
      .when(col("Provider Other Organization Name Type Code") === "3", "3, Doing Business As, O")
      .when(col("Provider Other Organization Name Type Code") === "4", "4, Former Legal Business Name, O")
      .when(col("Provider Other Organization Name Type Code") === "5", "5, Other Name, B"))


    val df7 = df6.withColumn("Provider Other Last Name Type Code", when(col("Provider Other Last Name Type Code") === "1", "1, Former Name, I")
      .when(col("Provider Other Last Name Type Code") === "2", "2, Professional Name, I")
      .when(col("Provider Other Last Name Type Code") === "3", "3, Doing Business As, O")
      .when(col("Provider Other Last Name Type Code") === "4", "4, Former Legal Business Name, O")
      .when(col("Provider Other Last Name Type Code") === "5", "5, Other Name, B"))


    val df8 = df7.withColumn("Other Provider Identifier Type Code_10", when(col("Other Provider Identifier Type Code_10") === "01", "01, OTHER")
      .when(col("Other Provider Identifier Type Code_10") === "05", "05, MEDICAID"))
    //df8.show(false)
    val npiDataColumn = df8.select(Seq.empty[org.apache.spark.sql.Column] ++
      df8.columns.map(colName => col("`" + colName + "`").as(colName.toLowerCase.replace(" ", "_")))
      : _*)
    val finaldeactivation = npiDataColumn.withColumnRenamed("npi", "npi_id")

    val activeoutputDF = finaldeactivation.join(deactiveDF, finaldeactivation("npi_id") === deactiveDF("npi"), "left").where(deactiveDF("npi") isNull)
    val deactiveoutputDF = finaldeactivation.join(deactiveDF, finaldeactivation("npi_id") === deactiveDF("npi"), "inner")
    //val joincondition = npiDataColumn.col("NPI") === deactiveDF.col("NPI")
    //val jointype = "inner"
    //val deactiveoutputDF = npiDataColumn.join(deactiveDF, joincondition, jointype)
    deactiveoutputDF.show()

    //val joincondition1 = npiDataColumn.col("NPI") === deactiveDF.col("NPI")
    //val jointype1 = "left"
    //val activeoutputDF = npiDataColumn.join(deactiveDF, joincondition1, jointype1)
    activeoutputDF.show()

    val active = activeoutputDF.select(col("npi_id"),col("entity_type_code"),col("is_sole_proprietor"),col("other_provider_identifier_type_code_10"),col("provider_gender_code"),col("provider_other_last_name_type_code"),col("provider_other_organization_name_type_code"))

    val deactive = deactiveoutputDF.select(col("npi_id"),col("entity_type_code"),col("is_sole_proprietor"),col("other_provider_identifier_type_code_10"),col("provider_gender_code"),col("provider_other_last_name_type_code"),col("provider_other_organization_name_type_code"))

    active.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "sparkpoc")
      .option("table", "active")
      .mode("append")
      .save()

    deactive.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "sparkpoc")
      .option("table", "deactive")
      .mode("append")
      .save()


  }
}











  /*  val finaldf= npiDataColumn.withColumn("npi", col("npi").cast(StringType))
    //val udeactivated2 = finaldf.select(col("npi"), col("entity_type_code"), col("provider_gender_code"), col("is_sole_proprietor"),col("provider_other_organization_name_type_code"),col("provider_other_last_name_type_code"),col("other_provider_identifier_type_code_10"))
    val summa = finaldf.select(col("npi"),col("entity_type_code"))
    /*create table common(npi text primary key, entity_type_code text,provider_gender_code text,is_sole_proprietor text,provider_other_organization_name_type_code text,provider_other_last_name_type_code text, other_provider_identifier_type_code_10 text);
    //
    val finaldf= df8.withColumn("NPI", col("NPI").cast(StringType))
    val udeactivated2 = df8.select(col("NPI"), col("Entity Type Code"),
      col("Provider Organization Name (Legal Business Name)"), col("Provider Last Name (Legal Name)"))
    //udeactivated2.printSchema()
    udeactivated2.show()
    udeactivated2.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "deactivate", "keyspace" -> "sparkpoc"))
      .mode("append") // overwrite, ignore, error
      .save()*/
    summa.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "sparkpoc")
      .option("table", "common2")
      .mode("append")
      .save()
   /* finaldf.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "sample1", "keyspace" -> "poc"))
      .mode("append")
      .save()*/
    /*val result: Map[String, Long] = df8
      .map(record => record(0) + record(1) + record(2) + record(3))
      .zipWithIndex()
      .collectAsMap()
    result.show()


   df8.write
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("topic", "notify")
         //.option("checkpointLocation", "chk-point-dir")
         //.option("path","final_output")
         .save()

       df8.selectExpr("CAST(NPI AS STRING) as  key", "CAST(* AS STRING) as value")
         .write
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("topic", "topic10")
         .save()

       //dropfieldDF.show(false)
       val broadcastStates = spark.sparkContext.broadcast(gender)


       import spark.sqlContext.implicits._
       //val df = data.toDF(columns: _*

       val df2 = rawDF.map(row => {
         //val country = row.getString(2)
         val gender = row.getString(41)

         //val fullCountry = broadcastCountries.value.get(country).get
         val fullgender = broadcastStates.value.get(gender).get


         (row.getString(0), row.getString(1), fullgender)
       })

       df2.show(false)*/

/*
    //valjoin1 = dropfieldDF.join(deactiveDF, dropfieldDF("NPI") === deactiveDF("NPI"), "left").where(deactiveDF("NPI") isNull).show(false)
   val join2 = dropfieldDF.join(deactiveDF, dropfieldDF("NPI") === deactiveDF("NPI"), "inner").show(false)

    join2.write.option("header",true)
      .csv("output")
  }
}/*
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("File Streaming Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()



    val rawDF = spark.read
      .format("csv")
      .option("header","true")
      .option("path", "input")
      .option("maxFilesPerTrigger", 1)
      .load()




    val deactivationDF = spark.read
      .format("csv")
      .option("header","true")
      .option("path", "deactivation_input")
      .option("maxFilesPerTrigger", 1)
      .load()

    val dropfieldDF = rawDF.drop("Replacement NPI", "Healthcare Provider Primary Taxonomy Switch_11", "Provider License Number State Code_11", "Healthcare Provider Taxonomy Code_15", "Other Provider Identifier Type Code_19", "Other Provider Identifier Type Code_22", "Other Provider Identifier_33",
      "Other Provider Identifier Issuer_36", "Other Provider Identifier_42", "Other Provider Identifier_40", "Other Provider Identifier Issuer_46")


    val newdeactivation = deactivationDF.withColumnRenamed("NPI", "NPID")
    dropfieldDF.createOrReplaceTempView("dropfieldDF")
    newdeactivation.createOrReplaceTempView("DF")
    val joinDF3 = spark.sql("select * from dropfieldDF join DF on dropfieldDF.NPI == DF.NPID ")
    joinDF3.show()
    val joinDF2 = spark.sql("select * from dropfieldDF Left join DF on dropfieldDF.NPI == DF.NPID where DF.NPID is null")
    joinDF2.show()

    //val explodeDF = rawDF. selectExpr("NPI","Entity Type Code","Replacement NPI","Employer Identification Number (EIN)","Provider Organization Name (Legal Business Name)","Provider Last Name (Legal Name)","Provider First Name")
    /*val flattenedDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")*/

    /*val condition = dropfieldDF.col("NPI") === deactivationDF.col("NPI")
       val deactiveoutputDF = dropfieldDF.filter(condition)
       val activeoutputDF = dropfieldDF.filter(not(condition))*/


    //dropfieldDF.join(deactivationDF,dropfieldDF("NPI")===deactivationDF("NPI"),"left").show(false)
    //val activeoutputDF = dropfieldDF.except(deactivationDF)

    /*val raw = rawDF.writeStream
      .format("csv")
      .queryName("Flattened Invoice Writer")
      .outputMode("append")
      .option("path", "output")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    val deactive = deactiveoutputDF.repartition(1).writeStream
      .format("csv")
      .queryName("deactive")
      .outputMode("append")
      .option("path", "deactive_output")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()*/
*/
   /* val active = activeoutputDF.repartition(1).write
      .format("csv")
      .queryName("active")
      .outputMode("append")
      .option("path", "active_output")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()*/

    //deactiveoutputDF.repartition(1).writeStream
    //.format("csv")

    //logger.info("Flattened Invoice Writer started")
     //active.awaitTermination()

    //deactive.awaitTermination()

  }
}*/
 /*join2.coalesce(1)
  .write.format("com.databricks.spark.csv")
  .option("header", "true")
  .save("/your/location/mydata")*/*/