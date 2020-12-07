package samples

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.SparkSession
import samples.services.{Service1, Service2, Service3}

object SampleProgram {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

    //Launcher for services
    val serviceToLaunch = "s1"
    val df = spark.read.csv("inputPath")

    serviceToLaunch match {
      case "s1" => Service1.deleteLineWithId(df, "", "")
      case "s2" => Service2.hashIdColumn(df, "", "", "")
      case "s3" => Service3.getClientData(df, "", "", "")

    }
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
