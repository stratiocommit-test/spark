/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hive.service.auth;

public class HttpAuthenticationException extends Exception {

  private static final long serialVersionUID = 0;

  /**
   * @param cause original exception
   */
  public HttpAuthenticationException(Throwable cause) {
    super(cause);
  }

  /**
   * @param msg exception message
   */
  public HttpAuthenticationException(String msg) {
    super(msg);
  }

  /**
   * @param msg   exception message
   * @param cause original exception
   */
  public HttpAuthenticationException(String msg, Throwable cause) {
    super(msg, cause);
  }

}
