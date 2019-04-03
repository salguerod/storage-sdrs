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

package com.google.gcs.sdrs.service.rule.impl;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.storagetransfer.v1.Storagetransfer;
import com.google.api.services.storagetransfer.v1.model.TransferJob;
import com.google.api.services.storagetransfer.v1.model.TransferSpec;
import com.google.gcs.sdrs.RetentionRuleType;
import com.google.gcs.sdrs.SdrsApplication;
import com.google.gcs.sdrs.controller.validation.ValidationConstants;
import com.google.gcs.sdrs.dao.model.RetentionJob;
import com.google.gcs.sdrs.dao.model.RetentionRule;
import com.google.gcs.sdrs.service.rule.RuleExecutor;
import com.google.gcs.sdrs.util.CredentialsUtil;
import com.google.gcs.sdrs.util.PrefixGeneratorUtility;
import com.google.gcs.sdrs.util.RetentionUtil;
import com.google.gcs.sdrs.util.StsUtil;
import java.io.IOException;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of the Rule Executor interface that uses STS */
public class StsRuleExecutor implements RuleExecutor {

  public static StsRuleExecutor instance;
  static CredentialsUtil credentialsUtil = CredentialsUtil.getInstance();

  private final String DEFAULT_SUFFIX = "shadow";
  private final String DEFAULT_PROJECT_ID = "global-default";
  private final String DEFAULT_MAX_PREFIX_COUNT = "1000";
  private final String DEFAULT_LOOKBACK_IN_DAYS = "365";
  private final String[] DEFAULT_LOG_CAT_BUCKET_PREFIX = {"2017/", "2018/", "2019/", "2020/"};
  private String suffix;
  private String defaultProjectId;
  private int maxPrefixCount;
  private int lookBackInDays;
  Storagetransfer client;

  private static final Logger logger = LoggerFactory.getLogger(StsRuleExecutor.class);

  public static StsRuleExecutor getInstance() {
    if (instance == null) {
      try {
        instance = new StsRuleExecutor();
      } catch (IOException ex) {
        logger.error("Could not establish connection with STS: ", ex.getMessage());
        logger.error("Underlying error: ", ex.getCause().getMessage());
      }
    }

    return instance;
  }

  /**
   * STS Rule Executor constructor that reads the bucket suffix from the configuration file
   *
   * @throws IOException when the STS Client cannot be instantiated
   */
  private StsRuleExecutor() throws IOException {
    suffix = SdrsApplication.getAppConfigProperty("sts.suffix", DEFAULT_SUFFIX);
    maxPrefixCount =
        Integer.valueOf(
            SdrsApplication.getAppConfigProperty("sts.maxPrefixCount", DEFAULT_MAX_PREFIX_COUNT));
    defaultProjectId =
        SdrsApplication.getAppConfigProperty("sts.defaultProjectId", DEFAULT_PROJECT_ID);
    lookBackInDays =
        Integer.valueOf(
            SdrsApplication.getAppConfigProperty(
                "sts.maxLookBackInDays", DEFAULT_LOOKBACK_IN_DAYS));

    GoogleCredential credentials = credentialsUtil.getCredentials();
    client = StsUtil.createStsClient(credentials);
  }

  /**
   * Executes dataset retention rules
   *
   * @param datasetRules a list of dataset retention rules
   * @param projectId the project that the datasets belong to
   * @return
   * @throws IOException when communication can't be established with STS
   * @throws IllegalArgumentException when there is an invalid argument
   */
  @Override
  public List<RetentionJob> executeDatasetRule(
      Collection<RetentionRule> datasetRules, String projectId)
      throws IOException, IllegalArgumentException {
    List<RetentionJob> datasetRuleJobs = new ArrayList<>();
    // get all dataset rules for a bucket
    Map<String, List<RetentionRule>> bucketDatasetMap = new HashMap<>();
    for (RetentionRule rule : datasetRules) {
      String key = RetentionUtil.getBucketName(rule.getDataStorageName());

      if (bucketDatasetMap.containsKey(key)) {
        bucketDatasetMap.get(key).add(rule);
      } else {
        List<RetentionRule> retentionRules = new ArrayList<>();
        retentionRules.add(rule);
        bucketDatasetMap.put(key, retentionRules);
      }
    }

    for (String bucketName : bucketDatasetMap.keySet()) {
      ZonedDateTime zonedDateTimeNow = ZonedDateTime.now(Clock.systemUTC());
      List<String> prefixes = new ArrayList<>();

      RetentionRule tmpRule = null;
      // create prefixes from all dataset rules for a bucket
      for (RetentionRule datasetRule : bucketDatasetMap.get(bucketName)) {
        // TODO eshen remove tmpRule once retention_job table is changed. For now just get rule id
        // from one dataset rule
        if (tmpRule == null) {
          tmpRule = datasetRule;
        }

        if (datasetRule.getType() == RetentionRuleType.USER) {
          String prefix = RetentionUtil.getDatasetPath(datasetRule.getDataStorageName());
          prefix = prefix.substring(0, prefix.lastIndexOf("/") + 1);
          if (prefix.isEmpty()) {
            String message =
                String.format(
                    "The target %s is the root of a bucket. Can not delete a bucket",
                    datasetRule.getDataStorageName());
            logger.error(message);
            throw new IllegalArgumentException(message);
          }
          prefixes.add(prefix);
        } else {
          prefixes.addAll(
              PrefixGeneratorUtility.generateTimePrefixes(
                  RetentionUtil.getDatasetPath(datasetRule.getDataStorageName()),
                  zonedDateTimeNow.minusDays(lookBackInDays),
                  zonedDateTimeNow.minusDays(datasetRule.getRetentionPeriodInDays())));
        }
      }

      String sourceBucket = bucketName;
      String destinationBucket = bucketName.concat(suffix);
      String description = buildDescription(tmpRule, zonedDateTimeNow);

      logger.debug(
          String.format(
              "Creating STS job with projectId: %s, "
                  + "description: %s, source: %s, destination: %s",
              projectId, description, sourceBucket, destinationBucket));

      TransferJob job =
          StsUtil.createStsJob(
              client,
              projectId,
              sourceBucket,
              destinationBucket,
              prefixes,
              description,
              zonedDateTimeNow);

      datasetRuleJobs.add(buildRetentionJobEntity(job.getName(), tmpRule));
    }

    return datasetRuleJobs;
  }

  /**
   * @param defaultRule the default rule to execute
   * @param bucketDatasetRules any dataset rules that exist within the same bucket as the default
   *     rule
   * @param scheduledTime the recurring time at which you want the default rule to execute
   * @return A {@link Collection} of {@link RetentionJob} records
   */
  @Override
  public List<RetentionJob> executeDefaultRule(
      RetentionRule defaultRule,
      Collection<RetentionRule> bucketDatasetRules,
      ZonedDateTime scheduledTime)
      throws IOException, IllegalArgumentException {

    if (defaultRule.getType().equals(RetentionRuleType.DATASET)) {
      String message = "DATASET retention rule type is invalid for this function";
      logger.error(message);
      throw new IllegalArgumentException(message);
    }

    List<RetentionJob> defaultRuleJobs = new ArrayList<>();
    Map<String, Set<String>> prefixesToExcludeMap = RetentionUtil.getPrefixMap(bucketDatasetRules);
    for (String mapKey : prefixesToExcludeMap.keySet()) {
      String[] combinedString = mapKey.split(";");
      String projectId = combinedString[0];
      String sourceBucket = combinedString[1];
      String fullSourceBucket = ValidationConstants.STORAGE_PREFIX + sourceBucket;
      String destinationBucket = sourceBucket + suffix;
      String description = buildDescription(defaultRule, scheduledTime);
      List<String> prefixesToExclude = new ArrayList<>(prefixesToExcludeMap.get(mapKey));
      if (prefixesToExclude.isEmpty()) {
        prefixesToExclude.addAll(Arrays.asList(DEFAULT_LOG_CAT_BUCKET_PREFIX));
      }

      // STS has a restriction of 1000 values in any prefix collection. This should never happen.
      if (prefixesToExclude.size() > maxPrefixCount) {
        String message =
            String.format(
                "There are too many dataset rules associated with this bucket. "
                    + "A maximum of %s rules are allowed.",
                maxPrefixCount);
        logger.error(message);
        throw new IllegalArgumentException(message);
      }

      logger.debug(
          String.format(
              "Creating STS job with for rule %s, projectId: %s, "
                  + "description: %s, source: %s, destination: %s",
              defaultRule.getId(), projectId, description, sourceBucket, destinationBucket));

      TransferJob job =
          StsUtil.createDefaultStsJob(
              client,
              projectId,
              sourceBucket,
              destinationBucket,
              prefixesToExclude,
              description,
              scheduledTime,
              defaultRule.getRetentionPeriodInDays());

      RetentionJob retentionJob = buildRetentionJobEntity(job.getName(), defaultRule);
      // Save the job with the actual projectId it is being created for, not the fake global
      // projectId that is set on the retentionRule. Same for data storage.
      retentionJob.setRetentionRuleProjectId(projectId);
      retentionJob.setRetentionRuleDataStorageName(fullSourceBucket);
      defaultRuleJobs.add(retentionJob);
    }

    return defaultRuleJobs;
  }

  /**
   * Sends a request to update a previously scheduled recurring transfer job
   *
   * @param defaultJobs The existing retention job record associated with the default rule
   * @param defaultRule the default rule record to update
   * @param bucketDatasetRules a {@link Collection} of child dataset rules
   * @return the {@link RetentionJob} record that was updated or the original record if no update is
   *     required
   * @throws IOException if the {@link Storagetransfer} client can't establish a connection to STS
   * @throws IllegalArgumentException if the rule type is Dataset, if no existing transfer job
   *     exists, if more than 1000 prefixes are excluded, or if the projectId can't be determined
   */
  public List<RetentionJob> updateDefaultRule(
      List<RetentionJob> defaultJobs,
      RetentionRule defaultRule,
      Collection<RetentionRule> bucketDatasetRules)
      throws IOException, IllegalArgumentException {

    List<RetentionJob> updatedJobs = new ArrayList<>();
    for (RetentionJob defaultJob : defaultJobs) {
      // get the existing transfer job from STS
      TransferJob existingTransferJob = getGlobalTransferJob(defaultJob, defaultRule);

      // Get existing job from STS
      TransferSpec transferSpec = existingTransferJob.getTransferSpec();

      boolean retentionPeriodChanged = false;
      boolean prefixesToExcludeChanged = false;

      // Check if retention period changed
      String existingRetention =
          transferSpec.getObjectConditions().getMinTimeElapsedSinceLastModification();
      String updatedRetention =
          StsUtil.convertRetentionInDaysToDuration(defaultRule.getRetentionPeriodInDays());

      if (!existingRetention.equals(updatedRetention)) {
        transferSpec.getObjectConditions().setMinTimeElapsedSinceLastModification(updatedRetention);
        retentionPeriodChanged = true;
      }

      Map<String, Set<String>> prefixesToExcludeMap =
          RetentionUtil.getPrefixMap(bucketDatasetRules);

      String bucketName = RetentionUtil.getBucketName(defaultJob.getRetentionRuleDataStorageName());
      String projectId = defaultJob.getRetentionRuleProjectId();
      String mapKey = RetentionUtil.generatePrefixMapKey(projectId, bucketName);

      // check if prefixes to exclude changed
      List<String> existingExcludePrefixList =
          transferSpec.getObjectConditions().getExcludePrefixes();
      List<String> updatedPrefixesToExclude = new ArrayList<>(prefixesToExcludeMap.get(mapKey));
      if (updatedPrefixesToExclude.isEmpty()) {
        updatedPrefixesToExclude.addAll(Arrays.asList(DEFAULT_LOG_CAT_BUCKET_PREFIX));
      }

      if (!isSamePrefixList(existingExcludePrefixList, updatedPrefixesToExclude)) {
        transferSpec.getObjectConditions().setExcludePrefixes(updatedPrefixesToExclude);
        prefixesToExcludeChanged = true;
      }

      // only update if the retention period or prefix list has changed
      if (retentionPeriodChanged || prefixesToExcludeChanged) {
        // Build transfer job object
        TransferJob updatedJob = new TransferJob();

        String description = buildDescription(defaultRule, ZonedDateTime.now(Clock.systemUTC()));
        updatedJob.setDescription(description);
        updatedJob.setTransferSpec(transferSpec);
        updatedJob.setStatus("ENABLED");

        TransferJob returnedTransferJob =
            StsUtil.updateExistingJob(
                client,
                updatedJob,
                existingTransferJob.getName(),
                existingTransferJob.getProjectId());
        RetentionJob job = buildRetentionJobEntity(returnedTransferJob.getName(), defaultRule);
        // Set the returned job ID to the same as the existing job for updating
        job.setId(defaultJob.getId());
        job.setRetentionRuleProjectId(defaultJob.getRetentionRuleProjectId());
        job.setRetentionRuleDataStorageName(defaultJob.getRetentionRuleDataStorageName());
        updatedJobs.add(job);
      }
    }

    return updatedJobs;
  }

  public List<RetentionJob> cancelDefaultJobs(List<RetentionJob> jobs, RetentionRule defaultRule)
      throws IOException, IllegalArgumentException {

    List<RetentionJob> cancelledJobs = new ArrayList<>();
    for (RetentionJob jobToCancel : jobs) {
      // get the existing transfer job from STS
      TransferJob existingTransferJob = getGlobalTransferJob(jobToCancel, defaultRule);

      TransferJob jobToUpdate = new TransferJob();
      jobToUpdate.setStatus("DISABLED");
      jobToUpdate.setDescription(existingTransferJob.getDescription());
      jobToUpdate.setTransferSpec(existingTransferJob.getTransferSpec());

      TransferJob updatedTransferJob =
          StsUtil.updateExistingJob(
              client,
              jobToUpdate,
              existingTransferJob.getName(),
              existingTransferJob.getProjectId());
      RetentionJob updatedJob = buildRetentionJobEntity(updatedTransferJob.getName(), defaultRule);
      updatedJob.setId(jobToCancel.getId());
      updatedJob.setRetentionRuleProjectId(jobToCancel.getRetentionRuleProjectId());
      updatedJob.setRetentionRuleDataStorageName(jobToCancel.getRetentionRuleDataStorageName());
      cancelledJobs.add(updatedJob);
    }

    return cancelledJobs;
  }

  private TransferJob getGlobalTransferJob(RetentionJob job, RetentionRule defaultRule)
      throws IOException, IllegalArgumentException {
    if (defaultRule.getType() == RetentionRuleType.DATASET) {
      String message = "DATASET retention rule type is invalid for this function";
      logger.error(message);
      throw new IllegalArgumentException(message);
    }

    // get the existing transfer job from STS
    TransferJob existingTransferJob =
        StsUtil.getExistingJob(client, job.getRetentionRuleProjectId(), job.getName());

    if (existingTransferJob == null) {
      String message =
          String.format(
              "Update failed. The requested transfer job %s does not exist in STS", job.getName());
      logger.error(message);
      throw new IllegalArgumentException(message);
    }

    return existingTransferJob;
  }

  private String buildDescription(RetentionRule rule, ZonedDateTime scheduledTime) {
    String description;
    if (rule.getId() == null && rule.getVersion() == null) {
      // a null id and version indicates a user triggered rule. Set description accordingly
      description =
          String.format("Rule User %s %s", rule.getDataStorageName(), scheduledTime.toString());
    } else {
      description =
          String.format("Rule %s %s %s", rule.getId(), rule.getVersion(), scheduledTime.toString());
    }

    return description;
  }

  private boolean isSamePrefixList(List<String> oldList, List<String> newList) {
    if (oldList == null && newList == null) {
      return true;
    }

    if (oldList == null || newList == null) {
      return false;
    }

    if (oldList.size() != newList.size()) {
      return false;
    }

    Collections.sort(oldList);
    Collections.sort(newList);

    if (oldList.equals(newList)) {
      return true;
    }

    return false;
  }

  private String extractProjectId(
      RetentionRule defaultRule, Collection<RetentionRule> datasetRules) {
    String projectId = defaultRule.getProjectId();
    // if the default rule doesn't have a projectId, get it from a child dataset rule
    if (defaultRule.getProjectId().isEmpty()
        || defaultRule.getProjectId().equalsIgnoreCase(defaultProjectId)) {
      Optional<RetentionRule> childRuleWithProject =
          datasetRules.stream().filter(r -> !r.getProjectId().isEmpty()).findFirst();
      if (childRuleWithProject.isPresent()) {
        projectId = childRuleWithProject.get().getProjectId();
      } else {
        String message = "STS job could not be created. No projectId found.";
        logger.error(message);
        throw new IllegalArgumentException(message);
      }
    }

    return projectId;
  }

  private List<String> buildExcludePrefixList(Collection<RetentionRule> datasetRules)
      throws IllegalArgumentException {

    List<String> prefixesToExclude = new ArrayList<>();
    for (RetentionRule datasetRule : datasetRules) {
      // Adds the dataset folder to the exclude list as the retention is already being handled
      // by the dataset rule. No need to generate the full prefix here.
      String pathToExclude = RetentionUtil.getDatasetPath(datasetRule.getDataStorageName());
      if (!pathToExclude.isEmpty()) {
        prefixesToExclude.add(pathToExclude);
      }
    }

    // STS has a restriction of 1000 values in any prefix collection. This should never happen.
    if (prefixesToExclude.size() > maxPrefixCount) {
      String message =
          String.format(
              "There are too many dataset rules associated with this bucket. "
                  + "A maximum of %s rules are allowed.",
              maxPrefixCount);
      logger.error(message);
      throw new IllegalArgumentException(message);
    }

    return prefixesToExclude;
  }

  RetentionJob buildRetentionJobEntity(String jobName, RetentionRule rule) {
    RetentionJob retentionJob = new RetentionJob();
    retentionJob.setName(jobName);
    retentionJob.setRetentionRuleId(rule.getId());
    retentionJob.setRetentionRuleProjectId(rule.getProjectId());
    retentionJob.setRetentionRuleDataStorageName(rule.getDataStorageName());
    retentionJob.setRetentionRuleType(rule.getType());
    retentionJob.setRetentionRuleVersion(rule.getVersion());

    return retentionJob;
  }
}
