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

import org.scalatra._
import scala.concurrent._
import scala.concurrent.duration._

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.session.{KyuubiSession, SessionHandle, SessionManager}

object SessionServlet extends Logging

abstract class SessionServlet[S<: KyuubiSession](private[kyuubi] val sessionManager: SessionManager)
  extends JsonServlet
//    with ApiVersioningSupport
    with MethodOverride
    with UrlGeneratorSupport
    with GZipSupport
{
  /**
    * Creates a new session based on the current request. The implementation is responsible for
    * parsing the body of the request.
    */
   protected def createSession(req: HttpServletRequest): SessionHandle

  /**
    * Returns a object representing the session data to be sent back to the client.
    */
  protected def clientSessionView(session: S, req: HttpServletRequest): Any = session

  before() {
    contentType = "application/json"
  }

  get("/") {
    "get function start..."
  }

  post("/") {
    "post function start..."
  }

  protected def getRequestPathInfo(request: HttpServletRequest): String = {
    if (request.getPathInfo != null && request.getPathInfo != "/") {
      request.getPathInfo
    } else {
      ""
    }
  }

  error {
    case e: IllegalArgumentException => BadRequest(e.getMessage)
  }

  /**
    * Returns the remote user for the given request. Separate method so that tests can override it.
    */
  protected def remoteUser(req: HttpServletRequest): String = req.getRemoteUser()



}
