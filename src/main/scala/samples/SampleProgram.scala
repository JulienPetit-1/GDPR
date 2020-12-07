package samples

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.SparkSession
import samples.services.{Service1, Service2}

object SampleProgram {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    //Avec ConfigParser et ReaderWriter
//    val configCli = ConfigParser.getConfigArgs(args)
//    val df = SparkReaderWriter.readData(configCli.inputPath, configCli.inputFormat, hasHeader = true, configCli.delimiter)
//    df.show()
//
//    val DfWithoutId = df.filter(col("ID") =!= configCli.id)
//    SparkReaderWriter.writeData(DfWithoutId, configCli.outputPath, configCli.outputFormat, overwrite = true, Seq())
//    DfWithoutId.show()

    implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

    //Service 1
    val data = spark.read.csv("")
    val result = Service1.filterClientId(data, "11224")

    result.write.csv("inputPath")

    //Service 2
    val resultService2 = Service2.hashIdColumn(data, "idClient")
    resultService2.write.csv("inputPath")
    }

}
