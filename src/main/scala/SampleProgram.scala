package samples

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import samples.config.JsonConfigProtocol.jsonConfig
import samples.config.{ConfigReader, JsonConfig}
import samples.utils.DataFrameSchema
import spray.json._

object SampleProgram {

  def main(args: Array[String]): Unit = {

    //Launcher for services
    //Add scopt command line

    Logger.getLogger("org").setLevel(Level.OFF)

    implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

    val serviceToLaunch = "s1"

    //Créer un schéma pour le dataframe manuellement
//    val schema = StructType(Seq(
//      StructField("movie_name", StringType),
//      StructField("number_of_views", LongType),
//      StructField("mark", DoubleType),
//      StructField("principal_actor", StringType)
//    ))
//
//    val df = spark.read.option("delimiter", ";").schema(schema).csv("donnees.csv")

    //Automatiser le schéma avec config json
    val confReader = ConfigReader.readConfig("conf/config.json")
    val configuration = confReader.parseJson.convertTo[JsonConfig]

    val dfSchema: StructType = DataFrameSchema.buildDataframeSchema(configuration.fields)
    val data = DataFrameReader.readCsv("donnees.csv", configuration.csvOptions, dfSchema)

    data.na.drop(Seq("movie_name"))

    data.printSchema()
    data.show

    //Execution des services
//    val serviceToLaunch = "s1"
    //    serviceToLaunch match {
    //      case "s1" => Service1.deleteLineWithId(df, "", "")
    //      case "s2" => Service2.hashIdColumn(df, "", "", "")
    //      case "s3" => Service3.getClientData(df, "", "", "")
    //
    //    }
  }

  //Avec ConfigParser et ReaderWriter pour Service 1
  //    val configCli = ConfigParser.getConfigArgs(args)
  //    val df = SparkReaderWriter.readData(configCli.inputPath, configCli.inputFormat, hasHeader = true, configCli.delimiter)
  //    df.show()
  //
  //    val DfWithoutId = df.filter(col("ID") =!= configCli.id)
  //    SparkReaderWriter.writeData(DfWithoutId, configCli.outputPath, configCli.outputFormat, overwrite = true, Seq())
  //    DfWithoutId.show()
}
