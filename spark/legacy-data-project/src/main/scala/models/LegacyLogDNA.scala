package models

import com.google.inject.internal.util.$Nullable

case class Sessionization(anonymous_id: String, name: String, browser_family: String, os_family: String, device_family: String)

case class ResultParquet(ano: String, mes: String, dia: String, id: String, description: String, @$Nullable count: Long)

