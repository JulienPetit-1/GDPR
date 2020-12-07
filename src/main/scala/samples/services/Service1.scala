package samples.services


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Service1 {
  def deleteLineWithId(df: DataFrame, id: String, idColumnName: String): DataFrame = {
    df.filter(col(idColumnName) =!= id)
  }
}