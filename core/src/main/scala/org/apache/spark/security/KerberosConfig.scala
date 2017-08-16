/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.security

import java.nio.file.{Files, Paths}
import java.nio.file.attribute.PosixFilePermissions
import javax.xml.bind.DatatypeConverter

import org.apache.spark.internal.Logging

object KerberosConfig extends Logging{

  def prepareEnviroment(vaultUrl: String,
                        vaultToken: String,
                        options: Map[String, String]): Map[String, String] = {
    val kerberosVaultPath = options.get("KERBEROS_VAULT_PATH")
    if(kerberosVaultPath.isDefined) {
      val (keytab64, principal) =
        VaultHelper.getKeytabPrincipalFromVault(vaultUrl, vaultToken, kerberosVaultPath.get)
      val keytabPath = getKeytabPrincipal(keytab64, principal)
      Map("principal" -> principal, "keytabPath" -> keytabPath)
    } else {
      logInfo(s"tying to get ssl secrets from vault for Kerberos but not found vault path," +
        s" skipping")
      Map[String, String]()
    }
  }

  private def getKeytabPrincipal(keytab64: String, principal: String): String = {
    val bytes = DatatypeConverter.parseBase64Binary(keytab64)
    val kerberosSecretFile = Files.createFile(Paths.get(s"/tmp/$principal.keytab"),
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
    kerberosSecretFile.toFile.deleteOnExit() // just to be sure
    val writePath = Files.write(kerberosSecretFile, bytes)
    writePath.toAbsolutePath.toString
  }

}
