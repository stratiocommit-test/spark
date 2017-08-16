/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.graphx;

import java.io.Serializable;

/**
 * Represents a subset of the fields of an [[EdgeTriplet]] or [[EdgeContext]]. This allows the
 * system to populate only those fields for efficiency.
 */
public class TripletFields implements Serializable {

  /** Indicates whether the source vertex attribute is included. */
  public final boolean useSrc;

  /** Indicates whether the destination vertex attribute is included. */
  public final boolean useDst;

  /** Indicates whether the edge attribute is included. */
  public final boolean useEdge;

  /** Constructs a default TripletFields in which all fields are included. */
  public TripletFields() {
    this(true, true, true);
  }

  public TripletFields(boolean useSrc, boolean useDst, boolean useEdge) {
    this.useSrc = useSrc;
    this.useDst = useDst;
    this.useEdge = useEdge;
  }

  /**
   * None of the triplet fields are exposed.
   */
  public static final TripletFields None = new TripletFields(false, false, false);

  /**
   * Expose only the edge field and not the source or destination field.
   */
  public static final TripletFields EdgeOnly = new TripletFields(false, false, true);

  /**
   * Expose the source and edge fields but not the destination field. (Same as Src)
   */
  public static final TripletFields Src = new TripletFields(true, false, true);

  /**
   * Expose the destination and edge fields but not the source field. (Same as Dst)
   */
  public static final TripletFields Dst = new TripletFields(false, true, true);

  /**
   * Expose all the fields (source, edge, and destination).
   */
  public static final TripletFields All = new TripletFields(true, true, true);
}
