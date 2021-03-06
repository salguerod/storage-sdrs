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

package com.google.gcs.sdrs.service.manager;

import static org.junit.Assert.assertEquals;

import com.google.gcs.sdrs.service.worker.BaseWorker;
import com.google.gcs.sdrs.service.worker.WorkerResult;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test class for JobManagerMonitor */
public class JobManagerMonitorTest {

  private JobManager instance;
  private JobManagerMonitor objectToTest;

  /** Set up steps before each test */
  @Before
  public void setUp() {
    instance = JobManager.getInstance();
    objectToTest = new JobManagerMonitor(instance);
  }

  /** Tear down steps after each test */
  @After
  public void tearDown() {
    instance.shutDownJobManagerNow();
  }

  /**
   * Tests the getWorkerResult method of the monitor thread. Should decrement the activeWorkerCount
   * in the jobManager.
   */
  @Test
  public void getWorkerResultTest() throws InterruptedException {
    int activeWorkers = instance.activeWorkerCount.get();
    BaseWorker worker =
        new BaseWorker(UUID.randomUUID().toString()) {
          @Override
          public void doWork() {
            workerResult.setStatus(WorkerResult.WorkerResultStatus.SUCCESS);
          }
        };
    instance.submitJob(worker);
    assertEquals(instance.activeWorkerCount.get(), activeWorkers + 1);
    objectToTest.getWorkerResults();
    assertEquals(instance.activeWorkerCount.get(), activeWorkers);
  }
}
