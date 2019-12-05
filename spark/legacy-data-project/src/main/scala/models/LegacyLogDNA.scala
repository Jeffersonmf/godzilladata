package models

import java.util.{HashMap, List, Map}

import com.fasterxml.jackson.annotation._
import com.google.inject.internal.util.$Nullable
import org.apache.commons.lang.builder.{EqualsBuilder, HashCodeBuilder, ToStringBuilder}

import scala.beans.BeanProperty

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(Array("_source"))
class LegacyLogDNA {

  @JsonProperty("_source")
  @BeanProperty
  var source: Source = _

  @JsonIgnore
  @BeanProperty
  var additionalProperties: Map[String, Any] = new HashMap[String, Any]()

  def withSource(source: Source): LegacyLogDNA = {
    this.source = source
    this
  }

  @JsonAnySetter
  def setAdditionalProperty(name: String, value: AnyRef) {
    this.additionalProperties.put(name, value)
  }

  def withAdditionalProperty(name: String, value: AnyRef): LegacyLogDNA = {
    this.additionalProperties.put(name, value)
    this
  }

  override def toString(): String = {
    new ToStringBuilder(this).append("source", source).append("additionalProperties", additionalProperties).toString
  }

  override def hashCode(): Int = {
    new HashCodeBuilder().append(source).append(additionalProperties).toHashCode()
  }

  override def equals(other: Any): Boolean = {
    if (other == this) {
      return true
    }
    if ((other.isInstanceOf[LegacyLogDNA]) == false) {
      return false
    }
    val rhs = other.asInstanceOf[LegacyLogDNA]
    new EqualsBuilder().append(source, rhs.source).append(additionalProperties, rhs.additionalProperties).isEquals
  }
}

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(Array("_host", "_logtype", "_mac", "_tag", "_file", "_line", "_ts", "_app", "_ip", "_ipremote", "level", "logsource", "program", "message", "_lid"))
class Source {

  @JsonProperty("_host")
  @BeanProperty
  var host: String = _

  @JsonProperty("_logtype")
  @BeanProperty
  var logtype: String = _

  @JsonProperty("_mac")
  @BeanProperty
  var mac: String = _

  @JsonProperty("_tag")
  @BeanProperty
  var tag: List[Any] = null

  @JsonProperty("_file")
  @BeanProperty
  var file: String = _

  @JsonProperty("_line")
  @BeanProperty
  var line: String = _

  @JsonProperty("_ts")
  @BeanProperty
  var ts: java.lang.Integer = _

  @JsonProperty("_app")
  @BeanProperty
  var app: String = _

  @JsonProperty("_ip")
  @BeanProperty
  var ip: String = _

  @JsonProperty("_ipremote")
  @BeanProperty
  var ipremote: String = _

  @JsonProperty("level")
  @BeanProperty
  var level: AnyRef = _

  @JsonProperty("logsource")
  @BeanProperty
  var logsource: String = _

  @JsonProperty("program")
  @BeanProperty
  var program: String = _

  @JsonProperty("message")
  @BeanProperty
  var message: String = _

  @JsonProperty("_lid")
  @BeanProperty
  var lid: String = _

  @JsonIgnore
  @BeanProperty
  var additionalProperties: Map[String, Any] = new HashMap[String, Any]()

  def withHost(host: String): Source = {
    this.host = host
    this
  }

  def withLogtype(logtype: String): Source = {
    this.logtype = logtype
    this
  }

  def withMac(mac: String): Source = {
    this.mac = mac
    this
  }

  def withTag(tag: List[Any]): Source = {
    this.tag = tag
    this
  }

  def withFile(file: String): Source = {
    this.file = file
    this
  }

  def withLine(line: String): Source = {
    this.line = line
    this
  }

  def withTs(ts: java.lang.Integer): Source = {
    this.ts = ts
    this
  }

  def withApp(app: String): Source = {
    this.app = app
    this
  }

  def withIp(ip: String): Source = {
    this.ip = ip
    this
  }

  def withIpremote(ipremote: String): Source = {
    this.ipremote = ipremote
    this
  }

  def withLevel(level: AnyRef): Source = {
    this.level = level
    this
  }

  def withLogsource(logsource: String): Source = {
    this.logsource = logsource
    this
  }

  def withProgram(program: String): Source = {
    this.program = program
    this
  }

  def withMessage(message: String): Source = {
    this.message = message
    this
  }

  def withLid(lid: String): Source = {
    this.lid = lid
    this
  }

  @JsonAnySetter
  def setAdditionalProperty(name: String, value: AnyRef) {
    this.additionalProperties.put(name, value)
  }

  def withAdditionalProperty(name: String, value: AnyRef): Source = {
    this.additionalProperties.put(name, value)
    this
  }

  override def toString(): String = {
    new ToStringBuilder(this).append("host", host).append("logtype", logtype).append("mac", mac).append("tag", tag).append("file", file).append("line", line).append("ts", ts).append("app", app).append("ip", ip).append("ipremote", ipremote).append("level", level).append("logsource", logsource).append("program", program).append("message", message).append("lid", lid).append("additionalProperties", additionalProperties).toString
  }

  override def hashCode(): Int = {
    new HashCodeBuilder().append(app).append(level).append(line).append(lid).append(ip).append(program).append(logsource).append(message).append(ipremote).append(mac).append(logtype).append(file).append(host).append(tag).append(additionalProperties).append(ts).toHashCode()
  }

  override def equals(other: Any): Boolean = {
    if (other == this) {
      return true
    }
    if ((other.isInstanceOf[Source]) == false) {
      return false
    }
    val rhs = other.asInstanceOf[Source]
    new EqualsBuilder().append(app, rhs.app).append(level, rhs.level).append(line, rhs.line).append(lid, rhs.lid).append(ip, rhs.ip).append(program, rhs.program).append(logsource, rhs.logsource).append(message, rhs.message).append(ipremote, rhs.ipremote).append(mac, rhs.mac).append(logtype, rhs.logtype).append(file, rhs.file).append(host, rhs.host).append(tag, rhs.tag).append(additionalProperties, rhs.additionalProperties).append(ts, rhs.ts).isEquals
  }
}

case class ResultParquet(ano: String, mes: String, dia: String, id: String, description: String, @$Nullable count: Long)