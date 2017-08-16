/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions;

/**
 * Expression information, will be used to describe a expression.
 */
public class ExpressionInfo {
    private String className;
    private String usage;
    private String name;
    private String extended;
    private String db;

    public String getClassName() {
        return className;
    }

    public String getUsage() {
        return usage;
    }

    public String getName() {
        return name;
    }

    public String getExtended() {
        return extended;
    }

    public String getDb() {
        return db;
    }

    public ExpressionInfo(String className, String db, String name, String usage, String extended) {
        this.className = className;
        this.db = db;
        this.name = name;
        this.usage = usage;
        this.extended = extended;
    }

    public ExpressionInfo(String className, String name) {
        this(className, null, name, null, null);
    }

    public ExpressionInfo(String className, String db, String name) {
        this(className, db, name, null, null);
    }
}
