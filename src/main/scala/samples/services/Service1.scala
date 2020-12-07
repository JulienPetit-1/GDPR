package samples.services


import org.apache.spark.sql.DataFrame

object Service1 {
  def filterClientId(df: DataFrame, clientIdToDelete: String): DataFrame = {
    df.filter(s"Client ID =!= $clientIdToDelete")
  }
}