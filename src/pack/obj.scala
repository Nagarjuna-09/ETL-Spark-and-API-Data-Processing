package pack

//Scala libraries for reading data from url
import java.security.cert.X509Certificate
import javax.net.ssl._
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

//Spark libraries for transofrming the read data
import org.apache. spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql. functions._

object obj {

  def main(args: Array[String]): Unit = {
    
    //required for writing the file from eclipse
    System.setProperty("hadoop.home.dir","D:\\hadoop")
    
//Scala code for reading data from url, data is read as string.
    val sslContext = SSLContext.getInstance("TLS")

    sslContext.init(null, Array(new X509TrustManager {
      override def getAcceptedIssuers: Array[X509Certificate] = Array.empty[X509Certificate]
      override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
      override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
    }), new java.security.SecureRandom())

    val hostnameVerifier = new HostnameVerifier {
      override def verify(s: String, sslSession: SSLSession): Boolean = true
    }

    val httpClient = HttpClients.custom().setSSLContext(sslContext).setSSLHostnameVerifier(hostnameVerifier).build()
    val content = EntityUtils.toString(httpClient.execute(new HttpGet("https://randomuser.me/api/0.8/?results=10")).getEntity)
    val urlstring = content.mkString
    //println(urlstring)
    
//Scala code for reading data from url ends
    
//Spark code required for transforming read data
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
  //Converting url string to RDD. parallelize() is used to convert the string into RDD
    
    val rdd = sc.parallelize(List(urlstring))
    
    rdd.foreach(println)
    
    val df = spark.read.json(rdd)
    
    df.show()
    df.printSchema()
    
    //exploding the array type in schema to convert into struct type
    
    val arrayflatten = df.withColumn("results",expr("explode(results)"))
    
    arrayflatten.show()
    arrayflatten.printSchema()
    
    // flattening the struct type
    
    val final_flatten = arrayflatten.select("nationality",
                                            "results.user.cell",
                                            "results.user.dob",
                                            "results.user.email",
                                            "results.user.gender",
                                            "results.user.location.*",
                                            "results.user.md5",
                                            "results.user.name.*",
                                            "results.user.password",
                                            "results.user.phone",
                                            "results.user.picture.*",
                                            "results.user.registered",
                                            "results.user.salt",
                                            "results.user.sha1",
                                            "results.user.sha256",
                                            "results.user.username",
                                            "seed",
                                            "version")
    
    println("======= Flattened web API data frame =======")                                        
    final_flatten.show(10)
    
    //importing and converting data in local AVRO file to a data frame
    
    //one step conversion of the data imported in any format from anywhere to a data frame
    // if reading csv, include .option("multiline",true) in the below command
    val avrodf = spark.read.format("avro").load("file:///C:/data_import/Sparkdata/projectsample.avro")
    
    println("======= local AVRO data's data frame =======") 
    avrodf.show()
    avrodf.printSchema()
    
    //removing the numericals from the username in webapi dataframe
    val numericals_removed_df = final_flatten.withColumn("username", regexp_replace(col("username"), "([0-9])", ""))
    
    println("======= Numericals removed data =======")
    numericals_removed_df.show()
    
    // do left join of the numerical's removed df with the df created from local AVRO data
    
    val leftjoin_df = avrodf.join(numericals_removed_df,Seq("username"),"left")
    
    println("======= Left Join Data =======")
    leftjoin_df.show()
    
    println("======= Nationality is notNull (nationality detected) =======")
    
    val nationality_found_customers = leftjoin_df.filter(col("nationality").isNotNull)
    nationality_found_customers.show()
    
    println("======= Nationality is Null (nationality not detected) =======")
    
    val nationality_notfound_customers = leftjoin_df.filter(col("nationality").isNull)
    nationality_notfound_customers.show()
    
    //Writing data to local disk
    nationality_found_customers.write.format("parquet").mode("append").save("file:///C:/data_import/proj_results/nationality_detected")
    nationality_notfound_customers.write.format("parquet").mode("append").save("file:///C:/data_import/proj_results/nationality_notdetected")
    
    
  }

}
