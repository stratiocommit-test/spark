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

import org.apache.spark.internal.Logging

object ConfigSecurity extends Logging{

  var vaultToken: Option[String] = None
  val vaultHost: Option[String] = sys.env.get("VAULT_HOST")
  val vaultUri: Option[String] = {
    (sys.env.get("VAULT_PROTOCOL"), vaultHost, sys.env.get("VAULT_PORT")) match {
      case (Some(vaultProtocol), Some(vaultHost), Some(vaultPort)) =>
        val vaultUri = s"$vaultProtocol://$vaultHost:$vaultPort"
        logDebug(s"vault uri: $vaultUri found, any Vault Connection will use it")
        Option(vaultUri)
      case _ =>
        logDebug("No Vault information found, any Vault Connection will fail")
        None
    }
  }

  def prepareEnvironment(vaultAppToken: Option[String] = None,
                         vaulHost: Option[String] = None): Map[String, String] = {

    logDebug(s"env VAR: ${sys.env.mkString("\n")}")
    val secretOptionsMap = ConfigSecurity.extractSecretFromEnv(sys.env)
    logDebug(s"secretOptionsMap: ${secretOptionsMap.mkString("\n")}")
    loadingConf(secretOptionsMap)
    vaultToken = if (vaultAppToken.isDefined) {
      vaultAppToken
    } else sys.env.get("VAULT_TOKEN")
    if(vaultToken.isDefined) {
      require(vaultUri.isDefined, "A proper vault host is required")
      logDebug(s"env VAR: ${sys.env.mkString("\n")}")
      prepareEnvironment(vaultUri.get, vaultToken.get, secretOptionsMap)
    }
    else Map()
  }

  private def extractConfFromEnv(env: Map[String, String]): Map[String,
    Map[String, String]] = {
    val sparkSecurityPrefix = "spark_security_"
    val extract: ((String, String)) => String = (keyValue: (String, String)) => {
      val (key, _) = keyValue
      key match {
        case key if key.toLowerCase.contains(sparkSecurityPrefix + "hdfs") => "hdfs"
        case _ => ""
      }
    }
    env.groupBy(extract).filter(_._2.exists(_._1.toLowerCase.contains("enable")))
      .flatMap{case (key, value) =>
        if (key.nonEmpty) Option((key, value.map{case (propKey, propValue) =>
          (propKey.split(sparkSecurityPrefix.toUpperCase).tail.head, propValue)
        }))
        else None
      }
  }

  private def extractSecretFromEnv(env: Map[String, String]): Map[String,
    Map[String, String]] = {
    val sparkSecurityPrefix = "spark_security_"

    val extract: ((String, String)) => String = (keyValue: (String, String)) => {
      val (key, _) = keyValue
      key match {
        case key if key.toLowerCase.contains(sparkSecurityPrefix + "hdfs") => "hdfs"
        case key if key.toLowerCase.contains(sparkSecurityPrefix + "kerberos") => "kerberos"
        case key if key.toLowerCase.contains(sparkSecurityPrefix + "datastore") => "datastore"
        case key if key.toLowerCase.contains(sparkSecurityPrefix + "kafka") => "kafka"
        case _ => ""

      }
    }

    env.groupBy(extract).filter(_._2.exists(_._1.toLowerCase.contains("enable")))
      .flatMap{case (key, value) =>
      if (key.nonEmpty) Option((key, value.map{case (propKey, propValue) =>
        (propKey.split(sparkSecurityPrefix.toUpperCase).tail.head, propValue)
      }))
      else None
    }
  }

  private def loadingConf(secretOptions: Map[String, Map[String, String]]): Unit = {
    secretOptions.foreach { case (key, options) =>
      key match {
        case "hdfs" =>
          HDFSConfig.prepareEnviroment(options)
          logDebug("Downloaded HDFS conf")
        case _ =>
      }
    }
  }


  private def prepareEnvironment(vaultHost: String,
                                vaultToken: String,
                                secretOptions: Map[String,
                                  Map[String, String]]): Map[String, String] = {
    val seqOp: (Map[String, String], (String, Map[String, String])) => Map[String, String] =
      (agg: Map[String, String], value: (String, Map[String, String])) => {
        val (key, options) = value
        val secretOptions = key match {
          case "kerberos" => KerberosConfig.prepareEnviroment(vaultHost,
            vaultToken,
            options)
          case "datastore" => SSLConfig.prepareEnvironment(vaultHost,
            vaultToken,
            SSLConfig.sslTypeDataStore,
            options)
          case "kafka" => SSLConfig.prepareEnvironment(vaultHost,
            vaultToken,
            SSLConfig.sslTypeKafkaStore,
            options)
          case _ => Map[String, String]()
       }
        secretOptions ++ agg
      }
    val combOp: (Map[String, String], Map[String, String]) => Map[String, String] =
      (agg1: Map[String, String], agg2: Map[String, String]) => agg1 ++ agg2

    secretOptions.aggregate(Map[String, String]())(seqOp, combOp)
  }

}
