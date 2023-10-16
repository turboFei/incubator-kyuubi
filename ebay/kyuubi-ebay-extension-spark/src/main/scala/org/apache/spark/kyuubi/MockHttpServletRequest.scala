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
package org.apache.spark.kyuubi

import java.io.BufferedReader
import java.security.Principal
import java.util
import java.util.Locale
import javax.servlet._
import javax.servlet.http._

/**
 * A mock HttpServletRequest used for render the execution page.
 * @param sqlId associated sql query id.
 */
class MockHttpServletRequest(sqlId: Long) extends HttpServletRequest {
  override def getParameter(name: String): String = {
    require("id".equals(name), throw new UnsupportedOperationException())
    sqlId.toString
  }

  override def getHeader(name: String): String = null

  /** not invoked methods */
  override def getScheme: String = null
  override def isRequestedSessionIdValid: Boolean = false
  override def isRequestedSessionIdFromUrl: Boolean = false
  override def getPathTranslated: String = null
  override def getQueryString: String = null
  override def getRemoteUser: String = null
  override def getUserPrincipal: Principal = null
  override def getAuthType: String = null
  override def getMethod: String = null
  override def getPathInfo: String = null
  override def logout(): Unit = {}
  override def login(s: String, s1: String): Unit = {}
  override def getDateHeader(s: String): Long = 0L
  override def isUserInRole(s: String): Boolean = false
  override def getCookies: Array[Cookie] = null
  override def getRequestedSessionId: String = null
  override def getRequestURI: String = null
  override def getRequestURL: StringBuffer = null
  override def isRequestedSessionIdFromCookie: Boolean = false
  override def getServletPath: String = null
  override def changeSessionId(): String = null
  override def isRequestedSessionIdFromURL: Boolean = false
  override def getHeaderNames: util.Enumeration[String] = null
  override def getParts: util.Collection[Part] = null
  override def getContextPath: String = null
  override def isAsyncStarted: Boolean = false
  override def getContentLengthLong: Long = 0L
  override def isAsyncSupported: Boolean = false
  override def getContentLength: Int = 0
  override def getLocalPort: Int = 0
  override def startAsync(
      servletRequest: ServletRequest,
      servletResponse: ServletResponse): AsyncContext = null
  override def getRemotePort: Int = 0
  override def getServerPort: Int = 0
  override def setAttribute(s: String, o: Any): Unit = {}
  override def isSecure: Boolean = false
  override def authenticate(httpServletResponse: HttpServletResponse): Boolean = false
  override def getHeaders(s: String): util.Enumeration[String] = null
  override def getSession(b: Boolean): HttpSession = null
  override def upgrade[T <: HttpUpgradeHandler](aClass: Class[T]): T = null.asInstanceOf[T]
  override def getRequestDispatcher(s: String): RequestDispatcher = null
  override def getParameterValues(s: String): Array[String] = null
  override def getRealPath(s: String): String = null
  override def getAttribute(s: String): AnyRef = null
  override def removeAttribute(s: String): Unit = {}
  override def setCharacterEncoding(s: String): Unit = {}
  override def getServletContext: ServletContext = null
  override def getAsyncContext: AsyncContext = null
  override def getDispatcherType: DispatcherType = null
  override def getInputStream: ServletInputStream = null
  override def startAsync(): AsyncContext = null
  override def getAttributeNames: util.Enumeration[String] = null
  override def getLocales: util.Enumeration[Locale] = null
  override def getParameterMap: util.Map[String, Array[String]] = null
  override def getParameterNames: util.Enumeration[String] = null
  override def getCharacterEncoding: String = null
  override def getContentType: String = null
  override def getLocalAddr: String = null
  override def getLocalName: String = null
  override def getReader: BufferedReader = null
  override def getRemoteAddr: String = null
  override def getRemoteHost: String = null
  override def getServerName: String = null
  override def getLocale: Locale = null
  override def getProtocol: String = null
  override def getPart(s: String): Part = null
  override def getSession: HttpSession = null
  override def getIntHeader(s: String): Int = 0
}
