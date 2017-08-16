/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.status.api.v1

import javax.ws.rs.container.{ContainerRequestContext, ContainerRequestFilter}
import javax.ws.rs.core.Response
import javax.ws.rs.ext.Provider

@Provider
private[v1] class SecurityFilter extends ContainerRequestFilter with UIRootFromServletContext {
  override def filter(req: ContainerRequestContext): Unit = {
    val user = Option(req.getSecurityContext.getUserPrincipal).map { _.getName }.orNull
    if (!uiRoot.securityManager.checkUIViewPermissions(user)) {
      req.abortWith(
        Response
          .status(Response.Status.FORBIDDEN)
          .entity(raw"""user "$user"is not authorized""")
          .build()
      )
    }
  }
}
