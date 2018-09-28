/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package yaooqinn.kyuubi.servlet

import java.io.File
import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration

import yaooqinn.kyuubi.servlet.ClientConf
import yaooqinn.kyuubi.servlet.ClientConf.ConfEntry
import yaooqinn.kyuubi.servlet.ClientConf.DeprecatedConf

object KyuubiServletConf {
  case class Entry(override val key: String, override val dflt: AnyRef) extends ConfEntry

  object Entry {
    def apply(key: String, dflt: Boolean): Entry = Entry(key, dflt: JBoolean)
    def apply(key: String, dflt: Int): Entry = Entry(key, dflt: Integer)
    def apply(key: String, dflt: Long): Entry = Entry(key, dflt: JLong)
  }

  val SSL_KEYSTORE = Entry("kyuubi.keystore", null)
  val REQUEST_HEADER_SIZE = Entry("kyuubi.server.request-header.size", 131072)
  val RESPONSE_HEADER_SIZE = Entry("kyuubi.server.response-header.size", 131072)

  val FILE_UPLOAD_MAX_SIZE = Entry("kyuubi.file.upload.max.size", 100L * 1024 * 1024)
  val ENVIRONMENT = Entry("kyuubi.environment", "production")

  val SERVER_HOST = Entry("kyuubi.server.host", "0.0.0.0")
  val SERVER_PORT = Entry("kyuubi.server.port", 8998)

  val SSL_KEYSTORE_PASSWORD = Entry("kyuubi.keystore.password", null)
  val SSL_KEY_PASSWORD = Entry("kyuubi.key-password", null)

  // Days to keep Kyuubi server request logs.
  val REQUEST_LOG_RETAIN_DAYS = Entry("kyuubi.server.request-log-retain.days", 5)

  case class DepConf(override val key: String,
                      override val version: String,
                      override val deprecationMessage: String = "") extends DeprecatedConf

  private val configsWithAlternatives: Map[String, DeprecatedConf] = Map[String, DepConf](
      // Todo
  )

  private val deprecatedConfigs: Map[String, DeprecatedConf] = {
    val configs: Seq[DepConf] = Seq(
      // Todo
    )

    Map(configs.map { cfg => (cfg.key -> cfg) }: _*)
  }

}

class KyuubiServletConf(loadDefaults: Boolean) extends ClientConf[KyuubiServletConf](null) {
  import KyuubiServletConf._

  def this() = this(true)

  override def getConfigsWithAlternatives: JMap[String, DeprecatedConf] = {
    configsWithAlternatives.asJava
  }

  override def getDeprecatedConfigs: JMap[String, DeprecatedConf] = {
    deprecatedConfigs.asJava
  }
}
