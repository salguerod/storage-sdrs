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
 *
 */

package com.google.gcs.sdrs.dao.impl;

import com.google.gcs.sdrs.dao.BaseDao;
import java.io.Serializable;
import java.util.List;
import javax.persistence.criteria.CriteriaQuery;
import org.hibernate.query.Query;
import org.hibernate.MultiIdentifierLoadAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Hibernate based Generic Dao implementation
 *
 * @param <T>
 * @param <Id>
 */
public class GenericDao<T, Id extends Serializable> extends BaseDao<T, Id> {

  private static final Logger logger = LoggerFactory.getLogger(GenericDao.class);

  public GenericDao(final Class<T> type) {
    super(type);
  }

  /* (non-Javadoc)
   * @see com.google.gcs.sdrs.dao.impl.DAO#saveOrUpdateBatch(List<T>)
   */
  @Override
  public void saveOrUpdateBatch(final List<T> entities) {
    openCurrentSessionWithTransaction();
    int i = 0;
    for (T entity : entities) {
      getCurrentSession().saveOrUpdate(entity);

      if (++i % 20 == 0) { // 20, same as the JDBC batch size
        // flush a batch of inserts and release memory:
        getCurrentSession().flush();
        getCurrentSession().clear();
      }
    }
    closeCurrentSessionWithTransaction();
  }

  /* (non-Javadoc)
   * @see com.google.gcs.sdrs.dao.impl.DAO#save(T)
   */
  @Override
  @SuppressWarnings("unchecked")
  public Id save(final T entity) {
    openCurrentSessionWithTransaction();
    Id result = (Id) getCurrentSession().save(entity);
    closeCurrentSessionWithTransaction();
    return result;
  }

  /* (non-Javadoc)
   * @see com.google.gcs.sdrs.dao.impl.DAO#update(T)
   */
  @Override
  public void update(final T entity) {
    openCurrentSessionWithTransaction();
    getCurrentSession().update(entity);
    closeCurrentSessionWithTransaction();
  }

  /* (non-Javadoc)
   * @see com.google.gcs.sdrs.dao.impl.DAO#findById(Id)
   */
  @Override
  public T findById(Id id) {
    openCurrentSession(); // no transaction per se for a find
    T entity = getCurrentSession().get(type, id);
    closeCurrentSession();
    return entity;
  }

  /* (non-Javadoc)
   * @see com.google.gcs.sdrs.dao.impl.DAO#findAllByMultipleIds(Class<T>, Collection<Id>)
   */
  @Override
  public List<T> findAllByMultipleIds(Class<T> cls, List<Id> ids) {
    openCurrentSession();
    MultiIdentifierLoadAccess<T> multiLoadAccess = getCurrentSession().byMultipleIds(cls);
    List<T> entities = multiLoadAccess.multiLoad(ids);
    closeCurrentSession();
    return entities;
  }

  /* (non-Javadoc)
   * @see com.google.gcs.sdrs.dao.impl.DAO#delete(T)
   */
  @Override
  public void delete(final T entity) {
    openCurrentSessionWithTransaction();
    getCurrentSession().delete(entity);
    closeCurrentSessionWithTransaction();
  }

  T getSingleRecordWithCriteriaQuery(CriteriaQuery<T> query) {
    Query<T> queryResults = getCurrentSession().createQuery(query);
    List<T> list = queryResults.getResultList();
    closeCurrentSession();

    T foundEntity = null;
    if(!list.isEmpty()){
      foundEntity = list.get(0);
    }
    return foundEntity;
  }
}
