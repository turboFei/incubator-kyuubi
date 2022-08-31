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

package org.apache.kyuubi.service.authentication

import javax.naming.{Context, NamingEnumeration, NamingException}
import javax.naming.directory.{InitialDirContext, SearchControls, SearchResult}
import javax.naming.ldap.InitialLdapContext
import javax.security.sasl.AuthenticationException

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.ServiceUtils

class LdapAuthenticationProviderImpl(conf: KyuubiConf) extends PasswdAuthenticationProvider {

  /**
   * The authenticate method is called by the Kyuubi Server authentication layer
   * to authenticate users for their requests.
   * If a user is to be granted, return nothing/throw nothing.
   * When a user is to be disallowed, throw an appropriate [[AuthenticationException]].
   *
   * @param user     The username received over the connection request
   * @param password The password received over the connection request
   *
   * @throws AuthenticationException When a user is found to be invalid by the implementation
   */
  override def authenticate(user: String, password: String): Unit = {
    if (StringUtils.isBlank(user)) {
      throw new AuthenticationException(s"Error validating LDAP user, user is null" +
        s" or contains blank space")
    }

    if (StringUtils.isBlank(password)) {
      throw new AuthenticationException(s"Error validating LDAP user, password is null" +
        s" or contains blank space")
    }

    val env = new java.util.Hashtable[String, Any]()
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.SECURITY_AUTHENTICATION, "simple")

    conf.get(AUTHENTICATION_LDAP_URL).foreach(env.put(Context.PROVIDER_URL, _))

    val domain = conf.get(AUTHENTICATION_LDAP_DOMAIN)
    val mail = if (!hasDomain(user) && domain.nonEmpty) (user + "@" + domain.get) else user

    conf.get(AUTHENTICATION_LDAP_BINDDN).map { bindDn =>
      val baseDn = conf.get(AUTHENTICATION_LDAP_BASEDN).getOrElse("")
      val bindPw = conf.get(AUTHENTICATION_LDAP_PASSWORD).getOrElse("")
      val attrs = conf.get(AUTHENTICATION_LDAP_ATTRIBUTES).toArray
      env.put(Context.SECURITY_PRINCIPAL, bindDn)
      env.put(Context.SECURITY_CREDENTIALS, bindPw)
      var nameEnuResults: NamingEnumeration[SearchResult] = null
      try {
        val ctx = new InitialLdapContext(env, null)
        val sc = new SearchControls
        sc.setReturningAttributes(attrs)
        sc.setSearchScope(SearchControls.SUBTREE_SCOPE)
        nameEnuResults = ctx.search(baseDn, s"(mail=$mail)", sc)
      } catch {
        case e: NamingException =>
          throw new AuthenticationException(
            s"LDAP InitialLdapContext failed, Error validating LDAP user: $user," +
              s" bindDn: $bindDn.",
            e)
      }
      if (nameEnuResults != null && nameEnuResults.hasMore) {
        try {
          val searchResult = nameEnuResults.next
          val attrs = searchResult.getAttributes.getAll
          if (!attrs.hasMore) {
            throw new AuthenticationException(
              s"LDAP attributes are empty, please check config " +
                s"AUTHENTICATION_LDAP_ATTRIBUTES.key, Error validating LDAP user: $user," +
                s" bindDn: $bindDn.")
          }
          while (attrs.hasMore) {
            attrs.next
            env.put(Context.SECURITY_PRINCIPAL, searchResult.getNameInNamespace)
            env.put(Context.SECURITY_CREDENTIALS, password)
            val ctx = new InitialDirContext(env)
            ctx.close()
          }
        } catch {
          case e: NamingException =>
            throw new AuthenticationException(
              s"LDAP InitialDirContext failed, Error validating LDAP user: $user," +
                s" bindDn: $bindDn.",
              e)
        }
      } else {
        throw new AuthenticationException(
          s"LDAP InitialLdapContext search results are empty, Error validating LDAP user: $user," +
            s" bindDn: $bindDn.")
      }
    }.getOrElse {
      val guidKey = conf.get(AUTHENTICATION_LDAP_GUIDKEY)
      val bindDn = conf.get(AUTHENTICATION_LDAP_BASEDN) match {
        case Some(dn) => guidKey + "=" + mail + "," + dn
        case _ => mail
      }
      env.put(Context.SECURITY_PRINCIPAL, bindDn)
      env.put(Context.SECURITY_CREDENTIALS, password)
      try {
        val ctx = new InitialDirContext(env)
        ctx.close()
      } catch {
        case e: NamingException =>
          throw new AuthenticationException(
            s"Error validating LDAP user: $user, bindDn: $bindDn.",
            e)
      }
    }
  }

  private def hasDomain(userName: String): Boolean = ServiceUtils.indexOfDomainMatch(userName) > 0
}
