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

import yaooqinn.kyuubi.Logging
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
      ,createRequest.username.getOrElse("default")
      ,createRequest.password.getOrElse("") ,
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

    while (!option.isFinished) {
      option = sessionManager.getOperationMgr.getOperation(opHandle)
    }

    var metadata = session.getResultSetMetadata(opHandle)
    var metadataString = ""
    var metadataArray = metadata.fieldNames
    for (name <- metadataArray) {
      metadataString += " | " + name
    }
    metadataString += " |  \n"



    var result = option.getResult()
    var iter = option.getIter()
    var statementId = option.getStatementId()

    var resultString = ""
    resultString += metadataString
    while(iter.hasNext) {
      var row = iter.next()
      for(i <- 0 to row.length-1) {resultString += " | " + row.get(i).toString}
      resultString  += " |  \n"
    }

     Map("statementId" -> statementId, "result" -> resultString)

  }

}
