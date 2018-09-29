/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.server

import java.util.concurrent.atomic.AtomicBoolean

import javax.servlet._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf}
import org.scalatra.metrics.MetricsBootstrap
import org.scalatra.servlet.{MultipartConfig, ServletApiImplicits}
import yaooqinn.kyuubi._
import yaooqinn.kyuubi.ha.HighAvailabilityUtils
import yaooqinn.kyuubi.service.{CompositeService, ServiceException}
import yaooqinn.kyuubi.servlet._
import yaooqinn.kyuubi.session.SessionManager

/**
 * Main entrance of Kyuubi Server
 */
private[kyuubi] class KyuubiServer private(name: String)
  extends CompositeService(name) with Logging {

  import KyuubiServletConf._

  // make kyuubi accessible for servlet
  private var server: WebServer = _
  private var _serverUrl: Option[String] = None
  private[kyuubi] var kyuubiServletConf: KyuubiServletConf = _

  private[this] var _beService: BackendService = _
  def beService: BackendService = _beService
  private[this] var _feService: FrontendService = _
  def feService: FrontendService = _feService

  private[this] val started = new AtomicBoolean(false)

  def this() = this(classOf[KyuubiServer].getSimpleName)

  override def init(conf: SparkConf): Unit = synchronized {
    this.conf = conf
    _beService = new BackendService()
    _feService = new FrontendService(_beService)
    addService(_beService)
    addService(_feService)
    super.init(conf)
  }

  override def start(): Unit = {
    super.start()
    started.set(true)
    kyuubiServletConf = new KyuubiServletConf()
    // make kyuubi accessible for servlet
    val host = kyuubiServletConf.get(SERVER_HOST)
    val port = kyuubiServletConf.getInt(SERVER_PORT)
    val multipartConfig = MultipartConfig(
      maxFileSize = Some(kyuubiServletConf.getLong(KyuubiServletConf.FILE_UPLOAD_MAX_SIZE))
    ).toMultipartConfigElement

    var sessionManager = _beService.getSessionManager

    server = new WebServer(kyuubiServletConf, host, port)
    server.context.setResourceBase("src/main/yaooqinn/kyuubi/server")
    // todo this may result some problems

    val kyuubiVersionServlet = new JsonServlet {
      before() { contentType = "application/json" }

      get("/") {
        Map("version" -> "0.1",
          "user" -> "john")
      }
    }

    server.context.addEventListener(
      new ServletContextListener() with MetricsBootstrap with ServletApiImplicits {
        private def mount(sc: ServletContext, servlet: Servlet, mappings: String*): Unit = {
          val registration = sc.addServlet(servlet.getClass().getName(), servlet)
          registration.addMapping(mappings: _*)
          registration.setMultipartConfig(multipartConfig)
        }

        override def contextDestroyed(servletContextEvent: ServletContextEvent): Unit = {}

        override def contextInitialized(servletContextEvent: ServletContextEvent): Unit = {
          try {
            val context = servletContextEvent.getServletContext()
            context.initParameters(org.scalatra.EnvironmentKey) = kyuubiServletConf.get(ENVIRONMENT)

            val kyuubiServlet = new KyuubiServlet(sessionManager)
            mount(context, kyuubiServlet, "/sessions/*")


            mount(context, kyuubiVersionServlet, "/version/*")
          } catch {
            case e: Throwable =>
              error("Exception thrown when initializing server", e)
              sys.exit(1)
          }
        }

      })

    server.start()

    Runtime.getRuntime().addShutdownHook(new Thread("Servlet Server Shutdown") {
      override def run(): Unit = {
        info("Shutting down Livy server.")
        server.stop()
      }
    })

    _serverUrl = Some(s"${server.protocol}://${server.host}:${server.port}")
    sys.props("livy.server.server-url") = _serverUrl.get

  }

  override def stop(): Unit = {
    if (started.getAndSet(false)) {
      super.stop()
    }
  }
}

object KyuubiServer extends Logging {
  def main(args: Array[String]): Unit = {
    startKyuubiServer()
  }

  def startKyuubiServer(): KyuubiServer = {
    try {
      KyuubiSparkUtil.initDaemon(logger)
      validate()
      val conf = new SparkConf(loadDefaults = true)
      KyuubiSparkUtil.setupCommonConfig(conf)
      val server = new KyuubiServer()
      KyuubiSparkUtil.addShutdownHook {
        () => server.stop()
      }
      server.init(conf)
      server.start()
      info(server.getName + " started!")
      if (HighAvailabilityUtils.isSupportDynamicServiceDiscovery(conf)) {
        info(s"HA mode: start to add this ${server.getName} instance to Zookeeper...")
        HighAvailabilityUtils.addServerInstanceToZooKeeper(server)
      }
      server
    } catch {
      case e: Exception => throw e
    }
  }

  private[kyuubi] def validate(): Unit = {
    if (KyuubiSparkUtil.majorVersion(KyuubiSparkUtil.SPARK_VERSION) < 2) {
      throw new ServiceException(s"${KyuubiSparkUtil.SPARK_VERSION} is too old for Kyuubi" +
        s" Server.")
    }
  }
}
