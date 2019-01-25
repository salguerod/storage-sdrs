/*
 * Copyright 2019 Google LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 * Any software provided by Google hereunder is distributed “AS IS”,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, and is not intended for production use.
 */

package com.google.gcs.sdrs.service;

import com.google.gcs.sdrs.controller.pojo.request.RetentionRuleCreateRequest;
import com.google.gcs.sdrs.dao.DAO;
import com.google.gcs.sdrs.dao.SingletonDao;
import com.google.gcs.sdrs.dao.model.RetentionRule;
import java.sql.Timestamp;

public class RetentionRulesService {

  DAO<RetentionRule, Integer> dao = SingletonDao.retentionRuleDAO;

  public Integer createRetentionRule(RetentionRuleCreateRequest rule) {
    RetentionRule entity = mapPojoToPersistenceEntity(rule);
    return dao.persist(entity);
  }

  private RetentionRule mapPojoToPersistenceEntity(RetentionRuleCreateRequest pojo) {
    RetentionRule entity = new RetentionRule();

    // Map over input values
    entity.setDatasetName(pojo.getDatasetName());
    entity.setDataStorageName(pojo.getDataStorageName());
    entity.setProjectId(pojo.getProjectId());
    entity.setRetentionPeriodInDays(pojo.getRetentionPeriod());
    entity.setType(pojo.getType());

    // Generate metadata
    Timestamp now = new Timestamp(System.currentTimeMillis());
    entity.setCreatedAt(now);
    entity.setUpdatedAt(now);
    entity.setIsActive(true);
    entity.setVersion(1);

    // TODO: pull actual user value from JWT
    entity.setUser("user");

    // TODO: remove when schema is updated
    if (entity.getDatasetName() == null) {
      entity.setDatasetName("not-nullable");
    }
    if (entity.getDataStorageName() == null) {
      entity.setDataStorageName("not-nullable");
    }
    if (entity.getProjectId() == null) {
      entity.setProjectId("not-nullable");
    }

    return entity;
  }
}
