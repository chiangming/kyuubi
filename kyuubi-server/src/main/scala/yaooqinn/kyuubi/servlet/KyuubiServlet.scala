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

import javax.servlet.http.HttpServletRequest

import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.scalatra._

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.session.{KyuubiSession, SessionHandle, SessionManager}



object KyuubiServlet extends Logging

class KyuubiServlet(sessionManager: SessionManager)
  extends SessionServlet(sessionManager) {

  override protected def createSession(req: HttpServletRequest): SessionHandle = {
    val createRequest = bodyAs[CreateRequest](req)

    // scalastyle:off println
    println("create Session :" + createRequest)
    // scalastyle:on println

    val sessionHandle = sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
      , createRequest.username.getOrElse("default")
      , createRequest.password.getOrElse(""),
      createRequest.ipAddress.getOrElse("0.0.0.0"),
      createRequest.sessionConf,
      withImpersonation = true)

    sessionHandle
  }

  post("/") {
    val sessionhandler = sessionManager.register(createSession(request))
    val session = sessionManager.getSession(sessionhandler)
    val sessionID = sessionhandler.getSessionId.toString

    Map("sessionID" -> sessionID)
  }

  post("/:sessionID/statement") {
    val statement = bodyAs[ExecuteRequest](request).code
    val sessionID = params("sessionID").toString
    val session = sessionManager.getSession(sessionID)
    val opHandle = session.executeStatement(statement)
    var option = sessionManager.getOperationMgr.getOperation(opHandle)

    while (!option.isTerminal) {
      option = sessionManager.getOperationMgr.getOperation(opHandle)
    }
    try {
      var metadata = session.getResultSetMetadata(opHandle)
      var metadataString = ""
      var resultMap: Map[String, Map[Int, String]] = Map()
      var line0: Map[Int, String] = Map()
      var metadataArray = metadata.fieldNames
      for (i <- 0 to metadataArray.length - 1) {
        metadataString += " | " + metadataArray(i)
        line0 += (i -> metadataArray(i))
      }
      resultMap += ("line0" -> line0)
      metadataString += " |  \n"


      var result = option.getResult()
      var iter = option.getIter()
      var statementId = option.getStatementId()

      var resultString = ""
      resultString += metadataString
      var line = 0
      while (iter.hasNext) {
        line += 1
        var row = iter.next()
        var lines: Map[Int, String] = Map()
        for (i <- 0 to row.length - 1) {
          resultString += " | " + row.get(i).toString
          lines += (i -> row.get(i).toString)
        }
        resultMap += (("line" + line) -> lines)
        resultString += " |  \n"
      }

      // Map("statementId" -> statementId, "resultToString" -> resultString, "result" -> resultMap)
      Map("statementId" -> statementId, "result" -> resultMap)
    } catch {
      case e => throw new KyuubiSQLException("Something wrong in this SQL statement," +
        " please check it. ")
    }
  }
}
