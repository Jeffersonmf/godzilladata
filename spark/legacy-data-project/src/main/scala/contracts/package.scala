import org.apache.spark.sql.types.{StringType, StructField, StructType}

package object contracts {

  def ordersFlashSchema = {StructType(Array(
    StructField("orderid", StringType, true),
    StructField("companyid", StringType, true),
    StructField("empresa", StringType, true),
    StructField("cnpj", StringType, true),
    StructField("flex", StringType, true),
    StructField("food", StringType, true),
    StructField("life", StringType, true),
    StructField("go", StringType, true),
    StructField("endereco", StringType, true),
    StructField("bairro", StringType, true),
    StructField("cep", StringType, true),
    StructField("cidade", StringType, true),
    StructField("estado", StringType, true),
    StructField("destinatario", StringType, true),
    StructField("date", StringType, true))
  )}

}
