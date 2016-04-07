/*
 * Copyright 2015 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.datamovement.impl;

import com.marklogic.datamovement.Forest;

public class ForestImpl implements Forest {
  private String hostName;
  private String databaseName;
  private String forestName;
  private String forestId;
  private boolean isUpdateable;
  private boolean isDeleteOnly;
  private long fragmentCount;

  public ForestImpl(String hostName, String databaseName, String forestName, String forestId,
    boolean isUpdateable, boolean isDeleteOnly, long fragmentCount)
  {
    this.hostName = hostName;
    this.databaseName  = databaseName;
    this.forestName    = forestName;
    this.forestId      = forestId;
    this.isUpdateable  = isUpdateable;
    this.isDeleteOnly  = isDeleteOnly;
    this.fragmentCount = fragmentCount;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getForestName() {
    return forestName;
  }

  public void setForestName(String forestName) {
    this.forestName = forestName;
  }

  public String getForestId() {
    return forestId;
  }

  public void setForestId(String forestId) {
    this.forestId = forestId;
  }

  public boolean isUpdateable() {
    return isUpdateable;
  }

  public void setIsUpdateable(boolean isUpdateable) {
    this.isUpdateable = isUpdateable;
  }

  public boolean isDeleteOnly() {
    return isDeleteOnly;
  }

  public void setIsDeleteOnly(boolean isDeleteOnly) {
    this.isDeleteOnly = isDeleteOnly;
  }

  public long getFragmentCount() {
    return fragmentCount;
  }

  public void setFragmentCount(long fragmentCount) {
    this.fragmentCount = fragmentCount;
  }

  public void increaseFragmentCount(long numFragments) {
    this.fragmentCount += numFragments;
  }

  public void decreaseFragmentCount(long numFragments) {
    this.fragmentCount -= numFragments;
  }

  public boolean equals(Object obj) {
    if ( obj == null ) return false;
    if ( ! (obj instanceof ForestImpl) ) return false;
    ForestImpl forestObj = (ForestImpl) obj;
    return getHostName()     == forestObj.getHostName() &&
           getDatabaseName() == forestObj.getDatabaseName() &&
           getForestId()   == forestObj.getForestId();
  }

  public int hashCode() {
    return (getHostName() != null ? getHostName().hashCode()     : 0) ^
      (getDatabaseName()  != null ? getDatabaseName().hashCode() : 0) ^
      (getForestId()    != null ? getForestId().hashCode()   : 0);
  }
}
