/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler.cluster.mesos

import org.apache.mesos.Protos.{ContainerInfo, Image, NetworkInfo, Volume}
import org.apache.mesos.Protos.ContainerInfo.DockerInfo
import org.apache.mesos.Protos.ContainerInfo.DockerInfo.Network

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging

/**
 * A collection of utility functions which can be used by both the
 * MesosSchedulerBackend and the [[MesosFineGrainedSchedulerBackend]].
 */
private[mesos] object MesosSchedulerBackendUtil extends Logging {
  /**
   * Parse a comma-delimited list of volume specs, each of which
   * takes the form [host-dir:]container-dir[:rw|:ro].
   */
  def parseVolumesSpec(volumes: String): List[Volume] = {
    volumes.split(",").map(_.split(":")).flatMap { spec =>
        val vol: Volume.Builder = Volume
          .newBuilder()
          .setMode(Volume.Mode.RW)
        spec match {
          case Array(container_path) =>
            Some(vol.setContainerPath(container_path))
          case Array(container_path, "rw") =>
            Some(vol.setContainerPath(container_path))
          case Array(container_path, "ro") =>
            Some(vol.setContainerPath(container_path)
              .setMode(Volume.Mode.RO))
          case Array(host_path, container_path) =>
            Some(vol.setContainerPath(container_path)
              .setHostPath(host_path))
          case Array(host_path, container_path, "rw") =>
            Some(vol.setContainerPath(container_path)
              .setHostPath(host_path))
          case Array(host_path, container_path, "ro") =>
            Some(vol.setContainerPath(container_path)
              .setHostPath(host_path)
              .setMode(Volume.Mode.RO))
          case spec =>
            logWarning(s"Unable to parse volume specs: $volumes. "
              + "Expected form: \"[host-dir:]container-dir[:rw|:ro](, ...)\"")
            None
      }
    }
    .map { _.build() }
    .toList
  }

  /**
   * Parse a comma-delimited list of port mapping specs, each of which
   * takes the form host_port:container_port[:udp|:tcp]
   *
   * Note:
   * the docker form is [ip:]host_port:container_port, but the DockerInfo
   * message has no field for 'ip', and instead has a 'protocol' field.
   * Docker itself only appears to support TCP, so this alternative form
   * anticipates the expansion of the docker form to allow for a protocol
   * and leaves open the chance for mesos to begin to accept an 'ip' field
   */
  def parsePortMappingsSpec(portmaps: String): List[DockerInfo.PortMapping] = {
    portmaps.split(",").map(_.split(":")).flatMap { spec: Array[String] =>
      val portmap: DockerInfo.PortMapping.Builder = DockerInfo.PortMapping
        .newBuilder()
        .setProtocol("tcp")
      spec match {
        case Array(host_port, container_port) =>
          Some(portmap.setHostPort(host_port.toInt)
            .setContainerPort(container_port.toInt))
        case Array(host_port, container_port, protocol) =>
          Some(portmap.setHostPort(host_port.toInt)
            .setContainerPort(container_port.toInt)
            .setProtocol(protocol))
        case spec =>
          logWarning(s"Unable to parse port mapping specs: $portmaps. "
            + "Expected form: \"host_port:container_port[:udp|:tcp](, ...)\"")
          None
      }
    }
    .map { _.build() }
    .toList
  }

  /**
   * Construct a DockerInfo structure and insert it into a ContainerInfo
   */
  def addDockerInfo(
      container: ContainerInfo.Builder,
      image: String,
      containerizer: String,
      forcePullImage: Boolean = false,
      volumes: Option[List[Volume]] = None,
      portmaps: Option[List[ContainerInfo.DockerInfo.PortMapping]] = None,
      networkName: Option[String] = None): Unit = {

    containerizer match {
      case "docker" =>
        container.setType(ContainerInfo.Type.DOCKER)
        val docker = ContainerInfo.DockerInfo.newBuilder()
          .setImage(image)
          .setForcePullImage(forcePullImage)
        // TODO (mgummelt): Remove this. Portmaps have no effect,
        //                  as we don't support bridge networking.
        portmaps.foreach(_.foreach(docker.addPortMappings))

        if (networkName.isDefined) {
          val name = networkName.get
          container.setDocker(docker.setNetwork(Network.USER))
            .addNetworkInfos(NetworkInfo.newBuilder().setName(name).build())
        } else container.setDocker(docker)

      case "mesos" =>
        container.setType(ContainerInfo.Type.MESOS)
        val imageProto = Image.newBuilder()
          .setType(Image.Type.DOCKER)
          .setDocker(Image.Docker.newBuilder().setName(image))
          .setCached(!forcePullImage)
        container.setMesos(ContainerInfo.MesosInfo.newBuilder().setImage(imageProto))
        ContainerInfo.MesosInfo.newBuilder()
        if (networkName.isDefined) {
          val name = networkName.get
          container.addNetworkInfos(NetworkInfo.newBuilder().setName(name).build())
        }
      case _ =>
        throw new SparkException(
          "spark.mesos.containerizer must be one of {\"docker\", \"mesos\"}")
    }

    volumes.foreach(_.foreach(container.addVolumes))
  }

  /**
   * Setup a docker containerizer from MesosDriverDescription scheduler properties
   */
  def setupContainerBuilderDockerInfo(
    imageName: String,
    conf: SparkConf,
    builder: ContainerInfo.Builder,
    networkName: Option[String] = None): Unit = {
    val forcePullImage = conf
      .getOption("spark.mesos.executor.docker.forcePullImage")
      .exists(_.equals("true"))
    val volumes = conf
      .getOption("spark.mesos.executor.docker.volumes")
      .map(parseVolumesSpec)
    val portmaps = conf
      .getOption("spark.mesos.executor.docker.portmaps")
      .map(parsePortMappingsSpec)

    val containerizer = conf.get("spark.mesos.containerizer", "docker")

    addDockerInfo(
      builder,
      imageName,
      containerizer,
      forcePullImage = forcePullImage,
      volumes = volumes,
      portmaps = portmaps,
      networkName)
    logDebug("setupContainerDockerInfo: using docker image: " + imageName)
  }
}
