/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClient;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * this class is the parent class of both the Kafka and Kinesis supervisor. All the main run loop
 * logic are similar enough so they're grouped together into this class.
 * <p>
 * Supervisor responsible for managing the SeekableStreamIndexTasks (Kafka/Kinesis) for a single dataSource. At a high level, the class accepts a
 * {@link SeekableStreamSupervisorSpec} which includes the stream name (topic / stream) and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the stream's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * stream sequences.
 * <p>
 *
 * @param <PartitionIdType>    the type of the partition id, for example, partitions in Kafka are int type while partitions in Kinesis are String type
 * @param <SequenceOffsetType> the type of the sequence number or offsets, for example, Kafka uses long offsets while Kinesis uses String sequence numbers
 */
public abstract class SeekableStreamSupervisor<PartitionIdType, SequenceOffsetType, RecordType extends ByteEntity> implements Supervisor
{
  public static final String CHECKPOINTS_CTX_KEY = "checkpoints";

  private static final long MINIMUM_GET_OFFSET_PERIOD_MILLIS = 5000;
  private static final long INITIAL_GET_OFFSET_DELAY_MILLIS = 15000;
  private static final long INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS = 25000;

  private static final long MAX_RUN_FREQUENCY_MILLIS = 1000;
  private static final long MINIMUM_FUTURE_TIMEOUT_IN_SECONDS = 120;
  private static final int MAX_INITIALIZATION_RETRIES = 20;

  private static final EmittingLogger log = new EmittingLogger(SeekableStreamSupervisor.class);
  private static ReentrantLock lock = new ReentrantLock(true);

  // Internal data structures
  // --------------------------------------------------------

  /**
   * A TaskGroup is the main data structure used by SeekableStreamSupervisor to organize and monitor stream partitions and
   * indexing tasks. All the tasks in a TaskGroup should always be doing the same thing (reading the same partitions and
   * starting from the same sequences) and if [replicas] is configured to be 1, a TaskGroup will contain a single task (the
   * exception being if the supervisor started up and discovered and adopted some already running tasks). At any given
   * time, there should only be up to a maximum of [taskCount] actively-reading task groups (tracked in the [activelyReadingTaskGroups]
   * map) + zero or more pending-completion task groups (tracked in [pendingCompletionTaskGroups]).
   */
  private class TaskGroup
  {
    final int groupId;

    // This specifies the partitions and starting sequences for this task group. It is set on group creation from the data
    // in [partitionGroups] and never changes during the lifetime of this task group, which will live until a task in
    // this task group has completed successfully, at which point this will be destroyed and a new task group will be
    // created with new starting sequences. This allows us to create replacement tasks for failed tasks that process the
    // same sequences, even if the values in [partitionGroups] has been changed.
    final ImmutableMap<PartitionIdType, SequenceOffsetType> startingSequences;

    // We don't include closed partitions in the starting offsets. However, we keep the full unfiltered map of
    // partitions, only used for generating the sequence name, to avoid ambiguity in sequence names if mulitple
    // task groups have nothing but closed partitions in their assignments.
    final ImmutableMap<PartitionIdType, SequenceOffsetType> unfilteredStartingSequencesForSequenceName;

    final ConcurrentHashMap<String, TaskData> tasks = new ConcurrentHashMap<>();
    final Optional<DateTime> minimumMessageTime;
    final Optional<DateTime> maximumMessageTime;
    final Set<PartitionIdType> exclusiveStartSequenceNumberPartitions;
    final TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>> checkpointSequences = new TreeMap<>();
    final String baseSequenceName;
    DateTime completionTimeout; // is set after signalTasksToFinish(); if not done by timeout, take corrective action

    TaskGroup(
        int groupId,
        ImmutableMap<PartitionIdType, SequenceOffsetType> startingSequences,
        @Nullable ImmutableMap<PartitionIdType, SequenceOffsetType> unfilteredStartingSequencesForSequenceName,
        Optional<DateTime> minimumMessageTime,
        Optional<DateTime> maximumMessageTime,
        @Nullable Set<PartitionIdType> exclusiveStartSequenceNumberPartitions
    )
    {
      this(
          groupId,
          startingSequences,
          unfilteredStartingSequencesForSequenceName,
          minimumMessageTime,
          maximumMessageTime,
          exclusiveStartSequenceNumberPartitions,
          generateSequenceName(
              unfilteredStartingSequencesForSequenceName == null
              ? startingSequences
              : unfilteredStartingSequencesForSequenceName,
              minimumMessageTime,
              maximumMessageTime,
              spec.getDataSchema(),
              taskTuningConfig
          )
      );
    }

    TaskGroup(
        int groupId,
        ImmutableMap<PartitionIdType, SequenceOffsetType> startingSequences,
        @Nullable ImmutableMap<PartitionIdType, SequenceOffsetType> unfilteredStartingSequencesForSequenceName,
        Optional<DateTime> minimumMessageTime,
        Optional<DateTime> maximumMessageTime,
        Set<PartitionIdType> exclusiveStartSequenceNumberPartitions,
        String baseSequenceName
    )
    {
      this.groupId = groupId;
      this.startingSequences = startingSequences;
      this.unfilteredStartingSequencesForSequenceName = unfilteredStartingSequencesForSequenceName == null
                                                        ? startingSequences
                                                        : unfilteredStartingSequencesForSequenceName;
      this.minimumMessageTime = minimumMessageTime;
      this.maximumMessageTime = maximumMessageTime;
      this.checkpointSequences.put(0, startingSequences);
      this.exclusiveStartSequenceNumberPartitions = exclusiveStartSequenceNumberPartitions != null
                                                    ? exclusiveStartSequenceNumberPartitions
                                                    : Collections.emptySet();
      this.baseSequenceName = baseSequenceName;
    }

    int addNewCheckpoint(Map<PartitionIdType, SequenceOffsetType> checkpoint)
    {
      checkpointSequences.put(checkpointSequences.lastKey() + 1, checkpoint);
      return checkpointSequences.lastKey();
    }

    Set<String> taskIds()
    {
      return tasks.keySet();
    }

  }

  private class TaskData
  {
    volatile TaskStatus status;
    volatile DateTime startTime;
    volatile Map<PartitionIdType, SequenceOffsetType> currentSequences = new HashMap<>();

    @Override
    public String toString()
    {
      return "TaskData{" +
             "status=" + status +
             ", startTime=" + startTime +
             ", checkpointSequences=" + currentSequences +
             '}';
    }
  }

  /**
   * Notice is used to queue tasks that are internal to the supervisor
   */
  private interface Notice
  {
    void handle() throws ExecutionException, InterruptedException, TimeoutException;
  }

  private static class StatsFromTaskResult
  {
    private final String groupId;
    private final String taskId;
    private final Map<String, Object> stats;

    public StatsFromTaskResult(
        int groupId,
        String taskId,
        Map<String, Object> stats
    )
    {
      this.groupId = String.valueOf(groupId);
      this.taskId = taskId;
      this.stats = stats;
    }

    public String getGroupId()
    {
      return groupId;
    }

    public String getTaskId()
    {
      return taskId;
    }

    public Map<String, Object> getStats()
    {
      return stats;
    }
  }

  private class RunNotice implements Notice
  {
    @Override
    public void handle()
    {
      long nowTime = System.currentTimeMillis();
      if (nowTime - lastRunTime < MAX_RUN_FREQUENCY_MILLIS) {
        return;
      }
      lastRunTime = nowTime;

      runInternal();
    }
  }

  // change taskCount without resubmitting.
  private class DynamicAllocationTasksNotice implements Notice
  {
    /**
     * This method will do lags points collection and check dynamic scale action is necessary or not.
     */
    @Override
    public void handle()
    {
      lock.lock();
      try {
        long nowTime = System.currentTimeMillis();
        // Only queue is full and over minTriggerDynamicFrequency can trigger scale out/in
        if (spec.isSuspended()) {
          log.info("[%s] supervisor is suspended, skip to check dynamic allocate task logic", dataSource);
          return;
        }
        log.debug("PendingCompletionTaskGroups is [%s] for dataSource [%s].", pendingCompletionTaskGroups, dataSource);
        for (CopyOnWriteArrayList list : pendingCompletionTaskGroups.values()) {
          if (!list.isEmpty()) {
            log.info("Still hand off tasks unfinished, skip to do scale action [%s] for dataSource [%s].", pendingCompletionTaskGroups, dataSource);
            return;
          }
        }
        if (nowTime - dynamicTriggerLastRunTime < minTriggerDynamicFrequency) {
          log.info("NowTime - dynamicTriggerLastRunTime is [%s]. Defined minTriggerDynamicFrequency is [%s] for dataSource [%s], CLAM DOWN NOW !", nowTime - dynamicTriggerLastRunTime, minTriggerDynamicFrequency, dataSource);
          return;
        }
        if (!lagMetricsQueue.isAtFullCapacity()) {
          log.info("Metrics collection is not at full capacity, may cause unnecessary scale. Skip to check dynamic allocate task : [%s] vs [%s]", lagMetricsQueue.size(), lagMetricsQueue.maxSize());
          return;
        }
        List<Long> lags = collectTotalLags();
        boolean allocationSuccess = dynamicAllocate(lags);
        if (allocationSuccess) {
          dynamicTriggerLastRunTime = nowTime;
          lagMetricsQueue.clear();
        }
      }
      catch (Exception e) {
        log.warn(e, "Error, when parse DynamicAllocationTasksNotice");
      }
      finally {
        lock.unlock();
      }
    }
  }

  /**
   * This method determines whether and how to do scale actions based on collected lag points.
   * Current algorithm of scale is simple:
   *    First of all, compute the proportion of lag points higher/lower than scaleOutThreshold/scaleInThreshold, getting scaleOutThreshold/scaleInThreshold.
   *    Secondly, compare scaleOutThreshold/scaleInThreshold with triggerScaleOutThresholdFrequency/triggerScaleInThresholdFrequency. P.S. Scale out action has higher priority than scale in action.
   *    Finaly, if scaleOutThreshold/scaleInThreshold is higher than triggerScaleOutThresholdFrequency/triggerScaleInThresholdFrequency, scale out/in action would be triggered.
   * If scale action is triggered :
   *    First of all, call gracefulShutdownInternal() which will change the state of  current datasource ingest tasks from reading to publishing.
   *    Secondly, clear all the stateful data structures: activelyReadingTaskGroups, partitionGroups, partitionOffsets, pendingCompletionTaskGroups, partitionIds. These structures will be rebuiled next 'RunNotice'.
   *    Finally, change taskCount in SeekableStreamSupervisorIOConfig and sync it to MetaStorage.
   * After changed taskCount in SeekableStreamSupervisorIOConfig, next RunNotice will ceate scaled number of ingest tasks without resubmitting supervisors.
   * @param lags the lag metrics of Stream(Kafka/Kinesis)
   * @return Boolean flag, do scale action successfully or not. If true , it will take at least 'minTriggerDynamicFrequency' before next 'dynamicAllocatie'.
   *         If false, it will do 'dynamicAllocate' again after 'dynamicCheckPeriod'.
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  private boolean dynamicAllocate(List<Long> lags) throws InterruptedException, ExecutionException, TimeoutException
  {
    // if supervisor is not suspended, ensure required tasks are running
    // if suspended, ensure tasks have been requested to gracefully stop
    log.info("[%s] supervisor is running, start to check dynamic allocate task logic. Current collected lags : [%s]", dataSource, lags);
    int beyond = 0;
    int within = 0;
    int metricsCount = lags.size();
    for (Long lag : lags) {
      if (lag >= scaleOutThreshold) {
        beyond++;
      }
      if (lag <= scaleInThreshold) {
        within++;
      }
    }
    double beyondProportion = beyond * 1.0 / metricsCount;
    double withinProportion = within * 1.0 / metricsCount;
    log.debug("triggerScaleOutThresholdFrequency is [%s] and triggerScaleInThresholdFrequency is [%s] for dataSource [%s].", triggerScaleOutThresholdFrequency, triggerScaleInThresholdFrequency, dataSource);
    log.info("beyondProportion is [%s] and withinProportion is [%s] for dataSource [%s].", beyondProportion, withinProportion, dataSource);

    int currentActiveTaskCount;
    int desireActiveTaskCount;
    Collection<TaskGroup> activeTaskGroups = activelyReadingTaskGroups.values();
    currentActiveTaskCount = activeTaskGroups.size();

    if (beyondProportion >= triggerScaleOutThresholdFrequency) {
      // Do Scale out
      int taskCount = currentActiveTaskCount + scaleOutStep;
      if (currentActiveTaskCount == taskCountMax) {
        log.info("CurrentActiveTaskCount reach task count Max limit, skip to scale out tasks for dataSource [%s].", dataSource);
        return false;
      } else {
        desireActiveTaskCount = Math.min(taskCount, taskCountMax);
      }
      log.debug("Start to scale out tasks, current active task number [%s] and desire task number is [%s] for dataSource [%s].", currentActiveTaskCount, desireActiveTaskCount, dataSource);
      gracefulShutdownInternal();
      // clear everything
      clearAllocationInfos();
      log.info("Change taskCount to [%s] for dataSource [%s].", desireActiveTaskCount, dataSource);
      changeTaskCountInIOConfig(desireActiveTaskCount);
      return true;
    }

    if (withinProportion >= triggerScaleInThresholdFrequency) {
      // Do Scale in
      int taskCount = currentActiveTaskCount - scaleInStep;
      if (currentActiveTaskCount == taskCountMin) {
        log.info("CurrentActiveTaskCount reach task count Min limit, skip to scale in tasks for dataSource [%s].", dataSource);
        return false;
      } else {
        desireActiveTaskCount = Math.max(taskCount, taskCountMin);
      }
      log.debug("Start to scale in tasks, current active task number [%s] and desire task number is [%s] for dataSource [%s].", currentActiveTaskCount, desireActiveTaskCount, dataSource);
      gracefulShutdownInternal();
      // clear everything
      clearAllocationInfos();
      log.info("Change taskCount to [%s] for dataSource [%s].", desireActiveTaskCount, dataSource);
      changeTaskCountInIOConfig(desireActiveTaskCount);
      return true;
    }
    return false;
  }

  private void changeTaskCountInIOConfig(int desireActiveTaskCount)
  {
    ioConfig.setTaskCount(desireActiveTaskCount);
    try {
      Optional<SupervisorManager> supervisorManager = taskMaster.getSupervisorManager();
      if (supervisorManager.isPresent()) {
        MetadataSupervisorManager metadataSupervisorManager = supervisorManager.get().getMetadataSupervisorManager();
        metadataSupervisorManager.insert(dataSource, spec);
      } else {
        log.warn("supervisorManager is null in taskMaster, skip to do scale action for dataSource [%s].", dataSource);
      }
    }
    catch (Exception e) {
      log.warn("Failed to sync taskCount to MetaStorage for dataSource [%s].", dataSource);
    }
  }

  private void clearAllocationInfos()
  {
    activelyReadingTaskGroups.clear();
    partitionGroups.clear();
    partitionOffsets.clear();

    pendingCompletionTaskGroups.clear();
    partitionIds.clear();
  }

  private List<Long> collectTotalLags()
  {
    return new ArrayList<>(lagMetricsQueue);
  }

  private class GracefulShutdownNotice extends ShutdownNotice
  {
    @Override
    public void handle() throws InterruptedException, ExecutionException, TimeoutException
    {
      gracefulShutdownInternal();
      super.handle();
    }
  }

  private class ShutdownNotice implements Notice
  {
    @Override
    public void handle() throws InterruptedException, ExecutionException, TimeoutException
    {
      recordSupplier.close();

      synchronized (stopLock) {
        stopped = true;
        stopLock.notifyAll();
      }
    }
  }

  private class ResetNotice implements Notice
  {
    final DataSourceMetadata dataSourceMetadata;

    ResetNotice(DataSourceMetadata dataSourceMetadata)
    {
      this.dataSourceMetadata = dataSourceMetadata;
    }

    @Override
    public void handle()
    {
      resetInternal(dataSourceMetadata);
    }
  }

  protected class CheckpointNotice implements Notice
  {
    private final int taskGroupId;
    private final SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> checkpointMetadata;

    CheckpointNotice(
        int taskGroupId,
        SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> checkpointMetadata
    )
    {
      this.taskGroupId = taskGroupId;
      this.checkpointMetadata = checkpointMetadata;
    }

    @Override
    public void handle() throws ExecutionException, InterruptedException
    {
      // check for consistency
      // if already received request for this sequenceName and dataSourceMetadata combination then return
      final TaskGroup taskGroup = activelyReadingTaskGroups.get(taskGroupId);

      if (isValidTaskGroup(taskGroupId, taskGroup)) {
        final TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>> checkpoints = taskGroup.checkpointSequences;

        // check validity of previousCheckpoint
        int index = checkpoints.size();
        for (int sequenceId : checkpoints.descendingKeySet()) {
          Map<PartitionIdType, SequenceOffsetType> checkpoint = checkpoints.get(sequenceId);
          // We have already verified the stream of the current checkpoint is same with that in ioConfig.
          // See checkpoint().
          if (checkpoint.equals(checkpointMetadata.getSeekableStreamSequenceNumbers()
                                                  .getPartitionSequenceNumberMap()
          )) {
            break;
          }
          index--;
        }
        if (index == 0) {
          throw new ISE("No such previous checkpoint [%s] found", checkpointMetadata);
        } else if (index < checkpoints.size()) {
          // if the found checkpoint is not the latest one then already checkpointed by a replica
          Preconditions.checkState(index == checkpoints.size() - 1, "checkpoint consistency failure");
          log.info("Already checkpointed with sequences [%s]", checkpoints.lastEntry().getValue());
          return;
        }
        final Map<PartitionIdType, SequenceOffsetType> newCheckpoint = checkpointTaskGroup(taskGroup, false).get();
        taskGroup.addNewCheckpoint(newCheckpoint);
        log.info("Handled checkpoint notice, new checkpoint is [%s] for taskGroup [%s]", newCheckpoint, taskGroupId);
      }
    }

    boolean isValidTaskGroup(int taskGroupId, @Nullable TaskGroup taskGroup)
    {
      if (taskGroup == null) {
        // taskGroup might be in pendingCompletionTaskGroups or partitionGroups
        if (pendingCompletionTaskGroups.containsKey(taskGroupId)) {
          log.warn(
              "Ignoring checkpoint request because taskGroup[%d] has already stopped indexing and is waiting for "
              + "publishing segments",
              taskGroupId
          );
          return false;
        } else if (partitionGroups.containsKey(taskGroupId)) {
          log.warn("Ignoring checkpoint request because taskGroup[%d] is inactive", taskGroupId);
          return false;
        } else {
          throw new ISE("Cannot find taskGroup [%s] among all activelyReadingTaskGroups [%s]", taskGroupId,
                        activelyReadingTaskGroups
          );
        }
      }

      return true;
    }
  }

  // Map<{group id}, {actively reading task group}>; see documentation for TaskGroup class
  private final ConcurrentHashMap<Integer, TaskGroup> activelyReadingTaskGroups = new ConcurrentHashMap<>();

  // After telling a taskGroup to stop reading and begin publishing a segment, it is moved from [activelyReadingTaskGroups] to here so
  // we can monitor its status while we queue new tasks to read the next range of sequences. This is a list since we could
  // have multiple sets of tasks publishing at once if time-to-publish > taskDuration.
  // Map<{group id}, List<{pending completion task groups}>>
  private final ConcurrentHashMap<Integer, CopyOnWriteArrayList<TaskGroup>> pendingCompletionTaskGroups = new ConcurrentHashMap<>();

  // We keep two separate maps for tracking the current state of partition->task group mappings [partitionGroups] and partition->offset
  // mappings [partitionOffsets]. The starting offset for a new partition in [partitionOffsets] is initially set to getNotSetMarker(). When a new task group
  // is created and is assigned partitions, if the offset for an assigned partition in [partitionOffsets] is getNotSetMarker() it will take the starting
  // offset value from the metadata store, and if it can't find it there, from stream. Once a task begins
  // publishing, the offset in [partitionOffsets] will be updated to the ending offset of the publishing-but-not-yet-
  // completed task, which will cause the next set of tasks to begin reading from where the previous task left
  // off. If that previous task now fails, we will set the offset in [partitionOffsets] back to getNotSetMarker() which will
  // cause successive tasks to again grab their starting offset from metadata store. This mechanism allows us to
  // start up successive tasks without waiting for the previous tasks to succeed and still be able to handle task
  // failures during publishing.
  protected final ConcurrentHashMap<PartitionIdType, SequenceOffsetType> partitionOffsets = new ConcurrentHashMap<>();
  protected final ConcurrentHashMap<Integer, Set<PartitionIdType>> partitionGroups = new ConcurrentHashMap<>();

  protected final ObjectMapper sortingMapper;
  protected final List<PartitionIdType> partitionIds = new CopyOnWriteArrayList<>();
  protected final SeekableStreamSupervisorStateManager stateManager;
  protected volatile DateTime sequenceLastUpdated;

  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  protected final String dataSource;

  private final Set<PartitionIdType> subsequentlyDiscoveredPartitions = new HashSet<>();
  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final SeekableStreamIndexTaskClient<PartitionIdType, SequenceOffsetType> taskClient;
  private final SeekableStreamSupervisorSpec spec;
  private final SeekableStreamSupervisorIOConfig ioConfig;
  private final Map<String, Object> dynamicAllocationTasksProperties;
  private final SeekableStreamSupervisorTuningConfig tuningConfig;
  private final SeekableStreamIndexTaskTuningConfig taskTuningConfig;
  private final String supervisorId;
  private final TaskInfoProvider taskInfoProvider;
  private final long futureTimeoutInSeconds; // how long to wait for async operations to complete
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final ExecutorService exec;
  private final ScheduledExecutorService scheduledExec;
  private final ScheduledExecutorService reportingExec;
  private final ScheduledExecutorService allocationExec;
  private final ScheduledExecutorService lagComputationExec;
  private final ListeningExecutorService workerExec;
  private final BlockingQueue<Notice> notices = new LinkedBlockingDeque<>();
  private final Object stopLock = new Object();
  private final Object stateChangeLock = new Object();
  private final ReentrantLock recordSupplierLock = new ReentrantLock();

  private final boolean useExclusiveStartingSequence;
  private boolean listenerRegistered = false;
  private long lastRunTime;
  private long dynamicTriggerLastRunTime;
  private int initRetryCounter = 0;
  private volatile DateTime firstRunTime;
  private volatile DateTime earlyStopTime = null;
  protected volatile RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> recordSupplier;
  private volatile boolean started = false;
  private volatile boolean stopped = false;
  private volatile boolean lifecycleStarted = false;
  private final ServiceEmitter emitter;
  private final boolean enableDynamicAllocationTasks;
  private long metricsCollectionIntervalMillis;
  private long metricsCollectionRangeMillis;
  private long scaleOutThreshold;
  private double triggerScaleOutThresholdFrequency;
  private long scaleInThreshold;
  private double triggerScaleInThresholdFrequency;
  private long dynamicCheckStartDelayMillis;
  private long dynamicCheckPeriod;
  private int taskCountMax;
  private int taskCountMin;
  private int scaleInStep;
  private int scaleOutStep;
  private long minTriggerDynamicFrequency;

  private volatile CircularFifoQueue<Long> lagMetricsQueue;

  public SeekableStreamSupervisor(
      final String supervisorId,
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final SeekableStreamIndexTaskClientFactory<? extends SeekableStreamIndexTaskClient<PartitionIdType, SequenceOffsetType>> taskClientFactory,
      final ObjectMapper mapper,
      final SeekableStreamSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory,
      final boolean useExclusiveStartingSequence
  )
  {
    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.sortingMapper = mapper.copy().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    this.spec = spec;
    this.emitter = spec.getEmitter();
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.useExclusiveStartingSequence = useExclusiveStartingSequence;
    this.dataSource = spec.getDataSchema().getDataSource();
    this.ioConfig = spec.getIoConfig();
    this.dynamicAllocationTasksProperties = ioConfig.getDynamicAllocationTasksProperties();
    log.debug("Get dynamicAllocationTasksProperties from IOConfig : [%s] in [%s]", dynamicAllocationTasksProperties, dataSource);
    if (dynamicAllocationTasksProperties != null && !dynamicAllocationTasksProperties.isEmpty() && Boolean.parseBoolean(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("enableDynamicAllocationTasks", false)))) {
      log.info("EnableDynamicAllocationTasks for datasource [%s]", dataSource);
      this.enableDynamicAllocationTasks = true;
    } else {
      log.info("Disable dynamic allocate tasks for [%s]", dataSource);
      this.enableDynamicAllocationTasks = false;
    }
    if (enableDynamicAllocationTasks) {
      this.metricsCollectionIntervalMillis = Long.parseLong(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("metricsCollectionIntervalMillis", 30000)));
      this.metricsCollectionRangeMillis = Long.parseLong(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("metricsCollectionRangeMillis", 600000)));
      int slots = (int) (metricsCollectionRangeMillis / metricsCollectionIntervalMillis) + 1;
      log.debug(" The interval of metrics collection is [%s], [%s] timeRange will collect [%s] data points for dataSource [%s].", metricsCollectionIntervalMillis, metricsCollectionRangeMillis, slots, dataSource);
      this.lagMetricsQueue = new CircularFifoQueue<>(slots);
      this.dynamicCheckStartDelayMillis = Long.parseLong(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("dynamicCheckStartDelayMillis", 300000)));
      this.dynamicCheckPeriod = Long.parseLong(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("dynamicCheckPeriod", 60000)));
      this.metricsCollectionRangeMillis = Long.parseLong(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("metricsCollectionRangeMillis", 600000)));
      this.scaleOutThreshold = Long.parseLong(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("scaleOutThreshold", 6000000)));
      this.scaleInThreshold = Long.parseLong(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("scaleInThreshold", 1000000)));
      this.triggerScaleOutThresholdFrequency = Double.parseDouble(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("triggerScaleOutThresholdFrequency", 0.3)));
      this.triggerScaleInThresholdFrequency = Double.parseDouble(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("triggerScaleInThresholdFrequency", 0.9)));
      this.taskCountMax = Integer.parseInt(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("taskCountMax", 4)));
      this.taskCountMin = Integer.parseInt(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("taskCountMin", 1)));
      this.scaleInStep = Integer.parseInt(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("scaleInStep", 1)));
      this.scaleOutStep = Integer.parseInt(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("scaleOutStep", 2)));
      this.minTriggerDynamicFrequency = Long.parseLong(String.valueOf(dynamicAllocationTasksProperties.getOrDefault("minTriggerDynamicFrequencyMillis", 600000)));
    }

    this.tuningConfig = spec.getTuningConfig();
    this.taskTuningConfig = this.tuningConfig.convertToTaskTuningConfig();
    this.supervisorId = supervisorId;
    this.exec = Execs.singleThreaded(StringUtils.encodeForFormat(supervisorId));
    this.scheduledExec = Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(supervisorId) + "-Scheduler-%d");
    this.reportingExec = Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(supervisorId) + "-Reporting-%d");
    this.allocationExec = Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(supervisorId) + "-Allocation-%d");
    this.lagComputationExec = Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(supervisorId) + "-Computation-%d");

    this.stateManager = new SeekableStreamSupervisorStateManager(
        spec.getSupervisorStateManagerConfig(),
        spec.isSuspended()
    );

    int workerThreads;
    if (enableDynamicAllocationTasks) {
      workerThreads = (this.tuningConfig.getWorkerThreads() != null
              ? this.tuningConfig.getWorkerThreads()
              : Math.min(10, taskCountMax));
    } else {
      workerThreads = (this.tuningConfig.getWorkerThreads() != null
              ? this.tuningConfig.getWorkerThreads()
              : Math.min(10, this.ioConfig.getTaskCount()));
    }

    this.workerExec = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            workerThreads,
            StringUtils.encodeForFormat(supervisorId) + "-Worker-%d"
        )
    );
    log.info("Created worker pool with [%d] threads for dataSource [%s]", workerThreads, this.dataSource);

    this.taskInfoProvider = new TaskInfoProvider()
    {
      @Override
      public TaskLocation getTaskLocation(final String id)
      {
        Preconditions.checkNotNull(id, "id");
        Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
        if (taskRunner.isPresent()) {
          Optional<? extends TaskRunnerWorkItem> item = Iterables.tryFind(
              taskRunner.get().getRunningTasks(),
              (Predicate<TaskRunnerWorkItem>) taskRunnerWorkItem -> id.equals(taskRunnerWorkItem.getTaskId())
          );

          if (item.isPresent()) {
            return item.get().getLocation();
          }
        } else {
          log.error("Failed to get task runner because I'm not the leader!");
        }

        return TaskLocation.unknown();
      }

      @Override
      public Optional<TaskStatus> getTaskStatus(String id)
      {
        return taskStorage.getStatus(id);
      }
    };

    this.futureTimeoutInSeconds = Math.max(
        MINIMUM_FUTURE_TIMEOUT_IN_SECONDS,
        tuningConfig.getChatRetries() * (tuningConfig.getHttpTimeout().getStandardSeconds()
                                         + IndexTaskClient.MAX_RETRY_WAIT_SECONDS)
    );
    int chatThreads;
    if (enableDynamicAllocationTasks) {
      chatThreads = (this.tuningConfig.getChatThreads() != null
              ? this.tuningConfig.getChatThreads()
              : Math.min(10, taskCountMax * this.ioConfig.getReplicas()));
    } else {
      chatThreads = (this.tuningConfig.getChatThreads() != null
              ? this.tuningConfig.getChatThreads()
              : Math.min(10, this.ioConfig.getTaskCount() * this.ioConfig.getReplicas()));
    }

    this.taskClient = taskClientFactory.build(
        taskInfoProvider,
        dataSource,
        chatThreads,
        this.tuningConfig.getHttpTimeout(),
        this.tuningConfig.getChatRetries()
    );
    log.info(
        "Created taskClient with dataSource[%s] chatThreads[%d] httpTimeout[%s] chatRetries[%d]",
        dataSource,
        chatThreads,
        this.tuningConfig.getHttpTimeout(),
        this.tuningConfig.getChatRetries()
    );
  }

  @Override
  public void start()
  {
    synchronized (stateChangeLock) {
      Preconditions.checkState(!lifecycleStarted, "already started");
      Preconditions.checkState(!exec.isShutdown(), "already stopped");

      // Try normal initialization first, if that fails then schedule periodic initialization retries
      try {
        tryInit();
      }
      catch (Exception e) {
        if (!started) {
          log.warn(
              "First initialization attempt failed for SeekableStreamSupervisor[%s], starting retries...",
              dataSource
          );

          exec.submit(
              () -> {
                try {
                  RetryUtils.retry(
                      () -> {
                        tryInit();
                        return 0;
                      },
                      (throwable) -> !started,
                      0,
                      MAX_INITIALIZATION_RETRIES,
                      null,
                      null
                  );
                }
                catch (Exception e2) {
                  log.makeAlert(
                      "Failed to initialize after %s retries, aborting. Please resubmit the supervisor spec to restart this supervisor [%s]",
                      MAX_INITIALIZATION_RETRIES,
                      supervisorId
                  ).emit();
                  throw new RuntimeException(e2);
                }
              }
          );
        }
      }
      lifecycleStarted = true;
    }
  }

  @Override
  public void stop(boolean stopGracefully)
  {
    synchronized (stateChangeLock) {
      Preconditions.checkState(lifecycleStarted, "lifecycle not started");

      log.info("Beginning shutdown of [%s]", supervisorId);
      stateManager.maybeSetState(SupervisorStateManager.BasicState.STOPPING);

      try {
        scheduledExec.shutdownNow(); // stop recurring executions
        reportingExec.shutdownNow();
        allocationExec.shutdownNow();
        lagComputationExec.shutdownNow();


        if (started) {
          Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
          if (taskRunner.isPresent()) {
            taskRunner.get().unregisterListener(supervisorId);
          }

          // Stopping gracefully will synchronize the end sequences of the tasks and signal them to publish, and will block
          // until the tasks have acknowledged or timed out. We want this behavior when we're explicitly shut down through
          // the API, but if we shut down for other reasons (e.g. we lose leadership) we want to just stop and leave the
          // tasks as they are.
          synchronized (stopLock) {
            if (stopGracefully) {
              log.info("Posting GracefulShutdownNotice, signalling managed tasks to complete and publish");
              notices.add(new GracefulShutdownNotice());
            } else {
              log.info("Posting ShutdownNotice");
              notices.add(new ShutdownNotice());
            }

            long shutdownTimeoutMillis = tuningConfig.getShutdownTimeout().getMillis();
            long endTime = System.currentTimeMillis() + shutdownTimeoutMillis;
            while (!stopped) {
              long sleepTime = endTime - System.currentTimeMillis();
              if (sleepTime <= 0) {
                log.info("Timed out while waiting for shutdown (timeout [%,dms])", shutdownTimeoutMillis);
                stopped = true;
                break;
              }
              stopLock.wait(sleepTime);
            }
          }
          log.info("Shutdown notice handled");
        }

        taskClient.close();
        workerExec.shutdownNow();
        exec.shutdownNow();
        started = false;

        log.info("[%s] has stopped", supervisorId);
      }
      catch (Exception e) {
        stateManager.recordThrowableEvent(e);
        log.makeAlert(e, "Exception stopping [%s]", supervisorId)
           .emit();
      }
    }
  }

  @Override
  public void reset(DataSourceMetadata dataSourceMetadata)
  {
    log.info("Posting ResetNotice");
    notices.add(new ResetNotice(dataSourceMetadata));
  }

  public ReentrantLock getRecordSupplierLock()
  {
    return recordSupplierLock;
  }


  @VisibleForTesting
  public void tryInit()
  {
    synchronized (stateChangeLock) {
      if (started) {
        log.warn("Supervisor was already started, skipping init");
        return;
      }

      if (stopped) {
        log.warn("Supervisor was already stopped, skipping init.");
        return;
      }

      try {
        recordSupplier = setupRecordSupplier();

        exec.submit(
            () -> {
              try {
                long pollTimeout = Math.max(ioConfig.getPeriod().getMillis(), MAX_RUN_FREQUENCY_MILLIS);
                while (!Thread.currentThread().isInterrupted() && !stopped) {
                  final Notice notice = notices.poll(pollTimeout, TimeUnit.MILLISECONDS);
                  if (notice == null) {
                    continue;
                  }

                  try {
                    notice.handle();
                  }
                  catch (Throwable e) {
                    stateManager.recordThrowableEvent(e);
                    log.makeAlert(e, "SeekableStreamSupervisor[%s] failed to handle notice", dataSource)
                       .addData("noticeClass", notice.getClass().getSimpleName())
                       .emit();
                  }
                }
              }
              catch (InterruptedException e) {
                stateManager.recordThrowableEvent(e);
                log.info("SeekableStreamSupervisor[%s] interrupted, exiting", dataSource);
              }
            }
        );
        firstRunTime = DateTimes.nowUtc().plus(ioConfig.getStartDelay());
        scheduledExec.scheduleAtFixedRate(
            buildRunTask(),
            ioConfig.getStartDelay().getMillis(),
            Math.max(ioConfig.getPeriod().getMillis(), MAX_RUN_FREQUENCY_MILLIS),
            TimeUnit.MILLISECONDS
        );

        scheduleReporting(reportingExec);
        if (enableDynamicAllocationTasks) {
          log.debug("Collect and compute lags at fixed rate of [%s] for dataSource[%s].", metricsCollectionIntervalMillis, dataSource);
          lagComputationExec.scheduleAtFixedRate(
                  collectAndComputeLags(),
                  dynamicCheckStartDelayMillis, // wait for tasks to start up
                  metricsCollectionIntervalMillis,
                  TimeUnit.MILLISECONDS
          );
          log.debug("allocate task at fixed rate of [%s], dataSource [%s].", dynamicCheckPeriod, dataSource);
          allocationExec.scheduleAtFixedRate(
                  buildDynamicAllocationTask(),
                  dynamicCheckStartDelayMillis + metricsCollectionRangeMillis,
                  dynamicCheckPeriod,
                  TimeUnit.MILLISECONDS
          );
        }
        started = true;
        log.info(
            "Started SeekableStreamSupervisor[%s], first run in [%s], with spec: [%s]",
            dataSource,
            ioConfig.getStartDelay(),
            spec.toString()
        );
      }
      catch (Exception e) {
        stateManager.recordThrowableEvent(e);
        if (recordSupplier != null) {
          recordSupplier.close();
        }
        initRetryCounter++;
        log.makeAlert(e, "Exception starting SeekableStreamSupervisor[%s]", dataSource)
           .emit();

        throw new RuntimeException(e);
      }
    }
  }

  /**
   * This method compute current consume lags. Get the total lags of all partition and fill in lagMetricsQueue
   * @return a Runnbale object to do collect and compute action.
   */
  private Runnable collectAndComputeLags()
  {
    return new Runnable() {
      @Override
      public void run()
      {
        lock.lock();
        try {
          if (!spec.isSuspended()) {
            ArrayList<Long> metricsInfo = new ArrayList<>(3);
            collectLag(metricsInfo);
            long totalLags = metricsInfo.size() < 3 ? 0 : metricsInfo.get(1);
            lagMetricsQueue.offer(totalLags > 0 ? totalLags : 0);
            log.debug("Current lag metric points [%s] for dataSource [%s].", new ArrayList<>(lagMetricsQueue), dataSource);
          } else {
            log.debug("[%s] supervisor is suspended, skip to collect kafka lags", dataSource);
          }
        }
        catch (Exception e) {
          log.warn(e, "Error, When collect kafka lags");
        }
        finally {
          lock.unlock();
        }
      }
    };
  }
  private Runnable buildDynamicAllocationTask()
  {
    return () -> notices.add(new DynamicAllocationTasksNotice());
  }

  private Runnable buildRunTask()
  {
    return () -> notices.add(new RunNotice());
  }

  @Override
  public SupervisorReport getStatus()
  {
    return generateReport(true);
  }


  @Override
  public SupervisorStateManager.State getState()
  {
    return stateManager.getSupervisorState();
  }

  @Override
  public Boolean isHealthy()
  {
    return stateManager.isHealthy();
  }

  private SupervisorReport<? extends SeekableStreamSupervisorReportPayload<PartitionIdType, SequenceOffsetType>> generateReport(
      boolean includeOffsets
  )
  {
    int numPartitions = partitionGroups.values().stream().mapToInt(Set::size).sum();

    final SeekableStreamSupervisorReportPayload<PartitionIdType, SequenceOffsetType> payload = createReportPayload(
        numPartitions,
        includeOffsets
    );

    SupervisorReport<SeekableStreamSupervisorReportPayload<PartitionIdType, SequenceOffsetType>> report = new SupervisorReport<>(
        dataSource,
        DateTimes.nowUtc(),
        payload
    );

    List<TaskReportData<PartitionIdType, SequenceOffsetType>> taskReports = new ArrayList<>();

    try {
      for (TaskGroup taskGroup : activelyReadingTaskGroups.values()) {
        for (Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
          String taskId = entry.getKey();
          @Nullable
          DateTime startTime = entry.getValue().startTime;
          Map<PartitionIdType, SequenceOffsetType> currentOffsets = entry.getValue().currentSequences;
          Long remainingSeconds = null;
          if (startTime != null) {
            long elapsedMillis = System.currentTimeMillis() - startTime.getMillis();
            long remainingMillis = Math.max(0, ioConfig.getTaskDuration().getMillis() - elapsedMillis);
            remainingSeconds = TimeUnit.MILLISECONDS.toSeconds(remainingMillis);
          }

          taskReports.add(
              new TaskReportData<>(
                  taskId,
                  includeOffsets ? taskGroup.startingSequences : null,
                  includeOffsets ? currentOffsets : null,
                  startTime,
                  remainingSeconds,
                  TaskReportData.TaskType.ACTIVE,
                  includeOffsets ? getRecordLagPerPartition(currentOffsets) : null,
                  includeOffsets ? getTimeLagPerPartition(currentOffsets) : null
              )
          );
        }
      }

      for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
        for (TaskGroup taskGroup : taskGroups) {
          for (Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
            String taskId = entry.getKey();
            @Nullable
            DateTime startTime = entry.getValue().startTime;
            Map<PartitionIdType, SequenceOffsetType> currentOffsets = entry.getValue().currentSequences;
            Long remainingSeconds = null;
            if (taskGroup.completionTimeout != null) {
              remainingSeconds = Math.max(0, taskGroup.completionTimeout.getMillis() - System.currentTimeMillis())
                                 / 1000;
            }

            taskReports.add(
                new TaskReportData<>(
                    taskId,
                    includeOffsets ? taskGroup.startingSequences : null,
                    includeOffsets ? currentOffsets : null,
                    startTime,
                    remainingSeconds,
                    TaskReportData.TaskType.PUBLISHING,
                    null,
                    null
                )
            );
          }
        }
      }

      taskReports.forEach(payload::addTask);
    }
    catch (Exception e) {
      log.warn(e, "Failed to generate status report");
    }

    return report;
  }

  @Override
  public Map<String, Map<String, Object>> getStats()
  {
    try {
      return getCurrentTotalStats();
    }
    catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      log.error(ie, "getStats() interrupted.");
      throw new RuntimeException(ie);
    }
    catch (ExecutionException | TimeoutException eete) {
      throw new RuntimeException(eete);
    }
  }

  /**
   * Collect row ingestion stats from all tasks managed by this supervisor.
   *
   * @return A map of groupId->taskId->task row stats
   *
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  private Map<String, Map<String, Object>> getCurrentTotalStats()
      throws InterruptedException, ExecutionException, TimeoutException
  {
    Map<String, Map<String, Object>> allStats = new HashMap<>();
    final List<ListenableFuture<StatsFromTaskResult>> futures = new ArrayList<>();
    final List<Pair<Integer, String>> groupAndTaskIds = new ArrayList<>();

    for (int groupId : activelyReadingTaskGroups.keySet()) {
      TaskGroup group = activelyReadingTaskGroups.get(groupId);
      for (String taskId : group.taskIds()) {
        futures.add(
            Futures.transform(
                taskClient.getMovingAveragesAsync(taskId),
                (Function<Map<String, Object>, StatsFromTaskResult>) (currentStats) -> new StatsFromTaskResult(
                    groupId,
                    taskId,
                    currentStats
                )
            )
        );
        groupAndTaskIds.add(new Pair<>(groupId, taskId));
      }
    }

    for (int groupId : pendingCompletionTaskGroups.keySet()) {
      List<TaskGroup> pendingGroups = pendingCompletionTaskGroups.get(groupId);
      for (TaskGroup pendingGroup : pendingGroups) {
        for (String taskId : pendingGroup.taskIds()) {
          futures.add(
              Futures.transform(
                  taskClient.getMovingAveragesAsync(taskId),
                  (Function<Map<String, Object>, StatsFromTaskResult>) (currentStats) -> new StatsFromTaskResult(
                      groupId,
                      taskId,
                      currentStats
                  )
              )
          );
          groupAndTaskIds.add(new Pair<>(groupId, taskId));
        }
      }
    }

    List<StatsFromTaskResult> results = Futures.successfulAsList(futures)
                                               .get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int i = 0; i < results.size(); i++) {
      StatsFromTaskResult result = results.get(i);
      if (result != null) {
        Map<String, Object> groupMap = allStats.computeIfAbsent(result.getGroupId(), k -> new HashMap<>());
        groupMap.put(result.getTaskId(), result.getStats());
      } else {
        Pair<Integer, String> groupAndTaskId = groupAndTaskIds.get(i);
        log.error("Failed to get stats for group[%d]-task[%s]", groupAndTaskId.lhs, groupAndTaskId.rhs);
      }
    }

    return allStats;
  }


  @VisibleForTesting
  public void addTaskGroupToActivelyReadingTaskGroup(
      int taskGroupId,
      ImmutableMap<PartitionIdType, SequenceOffsetType> partitionOffsets,
      Optional<DateTime> minMsgTime,
      Optional<DateTime> maxMsgTime,
      Set<String> tasks,
      Set<PartitionIdType> exclusiveStartingSequencePartitions
  )
  {
    TaskGroup group = new TaskGroup(
        taskGroupId,
        partitionOffsets,
        null,
        minMsgTime,
        maxMsgTime,
        exclusiveStartingSequencePartitions
    );
    group.tasks.putAll(tasks.stream().collect(Collectors.toMap(x -> x, x -> new TaskData())));
    if (activelyReadingTaskGroups.putIfAbsent(taskGroupId, group) != null) {
      throw new ISE(
          "trying to add taskGroup with id [%s] to actively reading task groups, but group already exists.",
          taskGroupId
      );
    }
  }

  @VisibleForTesting
  public void addTaskGroupToPendingCompletionTaskGroup(
      int taskGroupId,
      ImmutableMap<PartitionIdType, SequenceOffsetType> partitionOffsets,
      Optional<DateTime> minMsgTime,
      Optional<DateTime> maxMsgTime,
      Set<String> tasks,
      Set<PartitionIdType> exclusiveStartingSequencePartitions
  )
  {
    TaskGroup group = new TaskGroup(
        taskGroupId,
        partitionOffsets,
        null,
        minMsgTime,
        maxMsgTime,
        exclusiveStartingSequencePartitions
    );
    group.tasks.putAll(tasks.stream().collect(Collectors.toMap(x -> x, x -> new TaskData())));
    pendingCompletionTaskGroups.computeIfAbsent(taskGroupId, x -> new CopyOnWriteArrayList<>())
                               .add(group);
  }

  @VisibleForTesting
  public void runInternal()
  {
    try {
      possiblyRegisterListener();

      stateManager.maybeSetState(SeekableStreamSupervisorStateManager.SeekableStreamState.CONNECTING_TO_STREAM);
      if (!updatePartitionDataFromStream() && !stateManager.isAtLeastOneSuccessfulRun()) {
        return; // if we can't connect to the stream and this is the first run, stop and wait to retry the connection
      }

      stateManager.maybeSetState(SeekableStreamSupervisorStateManager.SeekableStreamState.DISCOVERING_INITIAL_TASKS);
      discoverTasks();

      updateTaskStatus();

      checkTaskDuration();

      checkPendingCompletionTasks();

      checkCurrentTaskState();

      // if supervisor is not suspended, ensure required tasks are running
      // if suspended, ensure tasks have been requested to gracefully stop
      if (!spec.isSuspended()) {
        log.info("[%s] supervisor is running.", dataSource);

        stateManager.maybeSetState(SeekableStreamSupervisorStateManager.SeekableStreamState.CREATING_TASKS);
        createNewTasks();
      } else {
        log.info("[%s] supervisor is suspended.", dataSource);
        gracefulShutdownInternal();
      }

      if (log.isDebugEnabled()) {
        log.debug(generateReport(true).toString());
      } else {
        log.info(generateReport(false).toString());
      }
    }
    catch (Exception e) {
      stateManager.recordThrowableEvent(e);
      log.warn(e, "Exception in supervisor run loop for dataSource [%s]", dataSource);
    }
    finally {
      stateManager.markRunFinished();
    }
  }

  private void possiblyRegisterListener()
  {
    if (listenerRegistered) {
      return;
    }

    Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
    if (taskRunner.isPresent()) {
      taskRunner.get().registerListener(
          new TaskRunnerListener()
          {
            @Override
            public String getListenerId()
            {
              return supervisorId;
            }

            @Override
            public void locationChanged(final String taskId, final TaskLocation newLocation)
            {
              // do nothing
            }

            @Override
            public void statusChanged(String taskId, TaskStatus status)
            {
              notices.add(new RunNotice());
            }
          }, Execs.directExecutor()
      );
      listenerRegistered = true;
    }
  }

  @VisibleForTesting
  public void gracefulShutdownInternal() throws ExecutionException, InterruptedException, TimeoutException
  {
    for (TaskGroup taskGroup : activelyReadingTaskGroups.values()) {
      for (Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
        if (taskInfoProvider.getTaskLocation(entry.getKey()).equals(TaskLocation.unknown())) {
          killTask(entry.getKey(), "Killing task for graceful shutdown");
        } else {
          entry.getValue().startTime = DateTimes.EPOCH;
        }
      }
    }

    checkTaskDuration();
  }

  @VisibleForTesting
  public void resetInternal(DataSourceMetadata dataSourceMetadata)
  {
    // clear queue for kafka lags
    if (enableDynamicAllocationTasks && lagMetricsQueue != null) {
      try {
        lock.lock();
        lagMetricsQueue.clear();
      }
      catch (Exception e) {
        log.warn(e, "Error,when clear queue in rest action");
      }
      finally {
        lock.unlock();
      }
    }

    if (dataSourceMetadata == null) {
      // Reset everything
      boolean result = indexerMetadataStorageCoordinator.deleteDataSourceMetadata(dataSource);
      log.info("Reset dataSource[%s] - dataSource metadata entry deleted? [%s]", dataSource, result);
      activelyReadingTaskGroups.values()
                               .forEach(group -> killTasksInGroup(
                                   group,
                                   "DataSourceMetadata is not found while reset"
                               ));
      activelyReadingTaskGroups.clear();
      partitionGroups.clear();
      partitionOffsets.clear();
    } else {
      if (!checkSourceMetadataMatch(dataSourceMetadata)) {
        throw new IAE(
            "Datasource metadata instance does not match required, found instance of [%s]",
            dataSourceMetadata.getClass()
        );
      }
      log.info("Reset dataSource[%s] with metadata[%s]", dataSource, dataSourceMetadata);
      // Reset only the partitions in dataSourceMetadata if it has not been reset yet
      @SuppressWarnings("unchecked")
      final SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> resetMetadata =
          (SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType>) dataSourceMetadata;

      if (resetMetadata.getSeekableStreamSequenceNumbers().getStream().equals(ioConfig.getStream())) {
        // metadata can be null
        final DataSourceMetadata metadata = indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(dataSource);
        if (metadata != null && !checkSourceMetadataMatch(metadata)) {
          throw new IAE(
              "Datasource metadata instance does not match required, found instance of [%s]",
              metadata.getClass()
          );
        }

        @SuppressWarnings("unchecked")
        final SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> currentMetadata =
            (SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType>) metadata;

        // defend against consecutive reset requests from replicas
        // as well as the case where the metadata store do not have an entry for the reset partitions
        boolean doReset = false;
        for (Entry<PartitionIdType, SequenceOffsetType> resetPartitionOffset : resetMetadata
            .getSeekableStreamSequenceNumbers()
            .getPartitionSequenceNumberMap()
            .entrySet()) {
          final SequenceOffsetType partitionOffsetInMetadataStore = currentMetadata == null
                                                                    ? null
                                                                    : currentMetadata
                                                                        .getSeekableStreamSequenceNumbers()
                                                                        .getPartitionSequenceNumberMap()
                                                                        .get(resetPartitionOffset.getKey());
          final TaskGroup partitionTaskGroup = activelyReadingTaskGroups.get(
              getTaskGroupIdForPartition(resetPartitionOffset.getKey())
          );
          final boolean isSameOffset = partitionTaskGroup != null
                                       && partitionTaskGroup.startingSequences.get(resetPartitionOffset.getKey())
                                                                              .equals(resetPartitionOffset.getValue());
          if (partitionOffsetInMetadataStore != null || isSameOffset) {
            doReset = true;
            break;
          }
        }

        if (!doReset) {
          log.info("Ignoring duplicate reset request [%s]", dataSourceMetadata);
          return;
        }

        boolean metadataUpdateSuccess;
        if (currentMetadata == null) {
          metadataUpdateSuccess = true;
        } else {
          final DataSourceMetadata newMetadata = currentMetadata.minus(resetMetadata);
          try {
            metadataUpdateSuccess = indexerMetadataStorageCoordinator.resetDataSourceMetadata(dataSource, newMetadata);
          }
          catch (IOException e) {
            log.error("Resetting DataSourceMetadata failed [%s]", e.getMessage());
            throw new RuntimeException(e);
          }
        }
        if (metadataUpdateSuccess) {
          resetMetadata.getSeekableStreamSequenceNumbers()
                       .getPartitionSequenceNumberMap()
                       .keySet()
                       .forEach(partition -> {
                         final int groupId = getTaskGroupIdForPartition(partition);
                         killTaskGroupForPartitions(
                             ImmutableSet.of(partition),
                             "DataSourceMetadata is updated while reset"
                         );
                         activelyReadingTaskGroups.remove(groupId);
                         // killTaskGroupForPartitions() cleans up partitionGroups.
                         // Add the removed groups back.
                         partitionGroups.computeIfAbsent(groupId, k -> new HashSet<>());
                         partitionOffsets.put(partition, getNotSetMarker());
                       });
        } else {
          throw new ISE("Unable to reset metadata");
        }
      } else {
        log.warn(
            "Reset metadata stream [%s] and supervisor's stream name [%s] do not match",
            resetMetadata.getSeekableStreamSequenceNumbers().getStream(),
            ioConfig.getStream()
        );
      }
    }
  }

  private void killTask(final String id, String reasonFormat, Object... args)
  {
    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      taskQueue.get().shutdown(id, reasonFormat, args);
    } else {
      log.error("Failed to get task queue because I'm not the leader!");
    }
  }

  private void killTaskWithSuccess(final String id, String reasonFormat, Object... args)
  {
    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      taskQueue.get().shutdownWithSuccess(id, reasonFormat, args);
    } else {
      log.error("Failed to get task queue because I'm not the leader!");
    }
  }

  private void killTasksInGroup(TaskGroup taskGroup, String reasonFormat, Object... args)
  {
    if (taskGroup != null) {
      for (String taskId : taskGroup.tasks.keySet()) {
        killTask(taskId, reasonFormat, args);
      }
    }
  }

  private void killTaskGroupForPartitions(Set<PartitionIdType> partitions, String reasonFormat, Object... args)
  {
    for (PartitionIdType partition : partitions) {
      int taskGroupId = getTaskGroupIdForPartition(partition);
      killTasksInGroup(activelyReadingTaskGroups.get(taskGroupId), reasonFormat, args);
      partitionGroups.remove(taskGroupId);
      activelyReadingTaskGroups.remove(taskGroupId);
    }
  }

  private boolean isTaskInPendingCompletionGroups(String taskId)
  {
    for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
      for (TaskGroup taskGroup : taskGroups) {
        if (taskGroup.tasks.containsKey(taskId)) {
          return true;
        }
      }
    }
    return false;
  }

  private void discoverTasks() throws ExecutionException, InterruptedException, TimeoutException
  {
    int taskCount = 0;
    List<String> futureTaskIds = new ArrayList<>();
    List<ListenableFuture<Boolean>> futures = new ArrayList<>();
    List<Task> tasks = taskStorage.getActiveTasksByDatasource(dataSource);

    final Map<Integer, TaskGroup> taskGroupsToVerify = new HashMap<>();

    for (Task task : tasks) {
      if (!doesTaskTypeMatchSupervisor(task)) {
        continue;
      }

      taskCount++;
      @SuppressWarnings("unchecked")
      final SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType> seekableStreamIndexTask = (SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType>) task;
      final String taskId = task.getId();

      // Check if the task has any inactive partitions. If so, terminate the task. Even if some of the
      // partitions assigned to the task are still active, we still terminate the task. We terminate such tasks early
      // to more rapidly ensure that all active partitions are evenly distributed and being read, and to avoid
      // having to map expired partitions which are no longer tracked in partitionIds to a task group.
      if (supportsPartitionExpiration()) {
        Set<PartitionIdType> taskPartitions = seekableStreamIndexTask.getIOConfig()
                                                                     .getStartSequenceNumbers()
                                                                     .getPartitionSequenceNumberMap()
                                                                     .keySet();
        Set<PartitionIdType> inactivePartitionsInTask = Sets.difference(
            taskPartitions,
            new HashSet<>(partitionIds)
        );
        if (!inactivePartitionsInTask.isEmpty()) {
          killTaskWithSuccess(
              taskId,
              "Task [%s] with partition set [%s] has inactive partitions [%s], stopping task.",
              taskId,
              taskPartitions,
              inactivePartitionsInTask
          );
          continue;
        }
      }

      // Determine which task group this task belongs to based on one of the partitions handled by this task. If we
      // later determine that this task is actively reading, we will make sure that it matches our current partition
      // allocation (getTaskGroupIdForPartition(partition) should return the same value for every partition being read
      // by this task) and kill it if it is not compatible. If the task is instead found to be in the publishing
      // state, we will permit it to complete even if it doesn't match our current partition allocation to support
      // seamless schema migration.

      Iterator<PartitionIdType> it = seekableStreamIndexTask.getIOConfig()
                                                            .getStartSequenceNumbers()
                                                            .getPartitionSequenceNumberMap()
                                                            .keySet()
                                                            .iterator();
      final Integer taskGroupId = (it.hasNext() ? getTaskGroupIdForPartition(it.next()) : null);

      if (taskGroupId != null) {
        // check to see if we already know about this task, either in [activelyReadingTaskGroups] or in [pendingCompletionTaskGroups]
        // and if not add it to activelyReadingTaskGroups or pendingCompletionTaskGroups (if status = PUBLISHING)
        TaskGroup taskGroup = activelyReadingTaskGroups.get(taskGroupId);

        if (!isTaskInPendingCompletionGroups(taskId) && (taskGroup == null || !taskGroup.tasks.containsKey(taskId))) {
          futureTaskIds.add(taskId);
          futures.add(
              Futures.transform(
                  taskClient.getStatusAsync(taskId),
                  new Function<SeekableStreamIndexTaskRunner.Status, Boolean>()
                  {
                    @Override
                    public Boolean apply(SeekableStreamIndexTaskRunner.Status status)
                    {
                      try {
                        log.debug("Task [%s], status [%s]", taskId, status);
                        if (status == SeekableStreamIndexTaskRunner.Status.PUBLISHING) {
                          seekableStreamIndexTask.getIOConfig()
                                                 .getStartSequenceNumbers()
                                                 .getPartitionSequenceNumberMap()
                                                 .keySet()
                                                 .forEach(
                                                     partition -> addDiscoveredTaskToPendingCompletionTaskGroups(
                                                         getTaskGroupIdForPartition(
                                                             partition),
                                                         taskId,
                                                         seekableStreamIndexTask.getIOConfig()
                                                                                .getStartSequenceNumbers()
                                                                                .getPartitionSequenceNumberMap()
                                                     ));

                          // update partitionGroups with the publishing task's sequences (if they are greater than what is
                          // existing) so that the next tasks will start reading from where this task left off
                          Map<PartitionIdType, SequenceOffsetType> publishingTaskEndOffsets = taskClient.getEndOffsets(
                              taskId);

                          // If we received invalid endOffset values, we clear the known offset to refetch the last committed offset
                          // from metadata. If any endOffset values are invalid, we treat the entire set as invalid as a safety measure.
                          boolean endOffsetsAreInvalid = false;
                          for (Entry<PartitionIdType, SequenceOffsetType> entry : publishingTaskEndOffsets.entrySet()) {
                            PartitionIdType partition = entry.getKey();
                            SequenceOffsetType sequence = entry.getValue();
                            if (sequence.equals(getEndOfPartitionMarker())) {
                              log.info(
                                  "Got end of partition marker for partition [%s] from task [%s] in discoverTasks, clearing partition offset to refetch from metadata..",
                                  taskId,
                                  partition
                              );
                              endOffsetsAreInvalid = true;
                              partitionOffsets.put(partition, getNotSetMarker());
                            }
                          }

                          if (!endOffsetsAreInvalid) {
                            for (Entry<PartitionIdType, SequenceOffsetType> entry : publishingTaskEndOffsets.entrySet()) {
                              PartitionIdType partition = entry.getKey();
                              SequenceOffsetType sequence = entry.getValue();

                              boolean succeeded;
                              do {
                                succeeded = true;
                                SequenceOffsetType previousOffset = partitionOffsets.putIfAbsent(partition, sequence);
                                if (previousOffset != null
                                    && (makeSequenceNumber(previousOffset)
                                    .compareTo(makeSequenceNumber(
                                        sequence))) < 0) {
                                  succeeded = partitionOffsets.replace(partition, previousOffset, sequence);
                                }
                              } while (!succeeded);
                            }
                          }
                        } else {
                          for (PartitionIdType partition : seekableStreamIndexTask.getIOConfig()
                                                                                  .getStartSequenceNumbers()
                                                                                  .getPartitionSequenceNumberMap()
                                                                                  .keySet()) {
                            if (!taskGroupId.equals(getTaskGroupIdForPartition(partition))) {
                              log.warn(
                                  "Stopping task [%s] which does not match the expected partition allocation",
                                  taskId
                              );
                              try {
                                stopTask(taskId, false)
                                    .get(futureTimeoutInSeconds, TimeUnit.SECONDS);
                              }
                              catch (InterruptedException | ExecutionException | TimeoutException e) {
                                stateManager.recordThrowableEvent(e);
                                log.warn(e, "Exception while stopping task");
                              }
                              return false;
                            }
                          }
                          // make sure the task's io and tuning configs match with the supervisor config
                          // if it is current then only create corresponding taskGroup if it does not exist
                          if (!isTaskCurrent(taskGroupId, taskId)) {
                            log.info(
                                "Stopping task [%s] which does not match the expected parameters and ingestion spec",
                                taskId
                            );
                            try {
                              stopTask(taskId, false)
                                  .get(futureTimeoutInSeconds, TimeUnit.SECONDS);
                            }
                            catch (InterruptedException | ExecutionException | TimeoutException e) {
                              stateManager.recordThrowableEvent(e);
                              log.warn(e, "Exception while stopping task");
                            }
                            return false;
                          } else {
                            final TaskGroup taskGroup = activelyReadingTaskGroups.computeIfAbsent(
                                taskGroupId,
                                k -> {
                                  log.info("Creating a new task group for taskGroupId[%d]", taskGroupId);
                                  // We reassign the task's original base sequence name (from the existing task) to the
                                  // task group so that the replica segment allocations are the same.
                                  return new TaskGroup(
                                      taskGroupId,
                                      ImmutableMap.copyOf(
                                          seekableStreamIndexTask.getIOConfig()
                                                                 .getStartSequenceNumbers()
                                                                 .getPartitionSequenceNumberMap()
                                      ),
                                      null,
                                      seekableStreamIndexTask.getIOConfig().getMinimumMessageTime(),
                                      seekableStreamIndexTask.getIOConfig().getMaximumMessageTime(),
                                      seekableStreamIndexTask.getIOConfig()
                                                             .getStartSequenceNumbers()
                                                             .getExclusivePartitions(),
                                      seekableStreamIndexTask.getIOConfig().getBaseSequenceName()
                                  );
                                }
                            );
                            taskGroupsToVerify.put(taskGroupId, taskGroup);
                            final TaskData prevTaskData = taskGroup.tasks.putIfAbsent(taskId, new TaskData());
                            if (prevTaskData != null) {
                              throw new ISE(
                                  "taskGroup[%s] already exists for new task[%s]",
                                  prevTaskData,
                                  taskId
                              );
                            }
                            verifySameSequenceNameForAllTasksInGroup(taskGroupId);
                          }
                        }
                        return true;
                      }
                      catch (Throwable t) {
                        stateManager.recordThrowableEvent(t);
                        log.error(t, "Something bad while discovering task [%s]", taskId);
                        return null;
                      }
                    }
                  }, workerExec
              )
          );
        }
      }
    }

    List<Boolean> results = Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int i = 0; i < results.size(); i++) {
      if (results.get(i) == null) {
        String taskId = futureTaskIds.get(i);
        killTask(taskId, "Task [%s] failed to return status, killing task", taskId);
      }
    }
    log.debug("Found [%d] seekablestream indexing tasks for dataSource [%s]", taskCount, dataSource);

    // make sure the checkpoints are consistent with each other and with the metadata store

    verifyAndMergeCheckpoints(taskGroupsToVerify.values());
  }

  private void verifyAndMergeCheckpoints(final Collection<TaskGroup> taskGroupsToVerify)
  {
    final List<ListenableFuture<?>> futures = new ArrayList<>();
    for (TaskGroup taskGroup : taskGroupsToVerify) {
      futures.add(workerExec.submit(() -> verifyAndMergeCheckpoints(taskGroup)));
    }
    try {
      Futures.allAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    }
    catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This method does two things -
   * 1. Makes sure the checkpoints information in the taskGroup is consistent with that of the tasks, if not kill
   * inconsistent tasks.
   * 2. truncates the checkpoints in the taskGroup corresponding to which segments have been published, so that any newly
   * created tasks for the taskGroup start indexing from after the latest published sequences.
   */
  private void verifyAndMergeCheckpoints(final TaskGroup taskGroup)
  {
    final int groupId = taskGroup.groupId;
    final List<Pair<String, TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>>>> taskSequences = new ArrayList<>();
    final List<ListenableFuture<TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>>>> futures = new ArrayList<>();
    final List<String> taskIds = new ArrayList<>();

    for (String taskId : taskGroup.taskIds()) {
      final ListenableFuture<TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>>> checkpointsFuture =
          taskClient.getCheckpointsAsync(
              taskId,
              true
          );
      futures.add(checkpointsFuture);
      taskIds.add(taskId);
    }

    try {
      List<TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>>> futuresResult =
          Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
      for (int i = 0; i < futuresResult.size(); i++) {
        final TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>> checkpoints = futuresResult.get(i);
        final String taskId = taskIds.get(i);
        if (checkpoints == null) {
          try {
            // catch the exception in failed futures
            futures.get(i).get();
          }
          catch (Exception e) {
            stateManager.recordThrowableEvent(e);
            log.error(e, "Problem while getting checkpoints for task [%s], killing the task", taskId);
            killTask(taskId, "Exception[%s] while getting checkpoints", e.getClass());
            taskGroup.tasks.remove(taskId);
          }
        } else if (checkpoints.isEmpty()) {
          log.warn("Ignoring task [%s], as probably it is not started running yet", taskId);
        } else {
          taskSequences.add(new Pair<>(taskId, checkpoints));
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    final DataSourceMetadata rawDataSourceMetadata = indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(
        dataSource);

    if (rawDataSourceMetadata != null && !checkSourceMetadataMatch(rawDataSourceMetadata)) {
      throw new IAE(
          "Datasource metadata instance does not match required, found instance of [%s]",
          rawDataSourceMetadata.getClass()
      );
    }

    @SuppressWarnings("unchecked")
    final SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> latestDataSourceMetadata = (SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType>) rawDataSourceMetadata;

    final boolean hasValidOffsetsFromDb = latestDataSourceMetadata != null &&
                                          latestDataSourceMetadata.getSeekableStreamSequenceNumbers() != null &&
                                          ioConfig.getStream().equals(
                                              latestDataSourceMetadata.getSeekableStreamSequenceNumbers().getStream()
                                          );
    final Map<PartitionIdType, SequenceOffsetType> latestOffsetsFromDb;
    if (hasValidOffsetsFromDb) {
      latestOffsetsFromDb = latestDataSourceMetadata.getSeekableStreamSequenceNumbers().getPartitionSequenceNumberMap();
    } else {
      latestOffsetsFromDb = null;
    }

    // order tasks of this taskGroup by the latest sequenceId
    taskSequences.sort((o1, o2) -> o2.rhs.firstKey().compareTo(o1.rhs.firstKey()));

    final Set<String> tasksToKill = new HashSet<>();
    final AtomicInteger earliestConsistentSequenceId = new AtomicInteger(-1);
    int taskIndex = 0;

    while (taskIndex < taskSequences.size()) {
      TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>> taskCheckpoints = taskSequences.get(taskIndex).rhs;
      String taskId = taskSequences.get(taskIndex).lhs;
      if (earliestConsistentSequenceId.get() == -1) {
        // find the first replica task with earliest sequenceId consistent with datasource metadata in the metadata
        // store
        if (taskCheckpoints.entrySet().stream().anyMatch(
            sequenceCheckpoint -> sequenceCheckpoint.getValue().entrySet().stream().allMatch(
                partitionOffset -> {
                  OrderedSequenceNumber<SequenceOffsetType> sequence = makeSequenceNumber(partitionOffset.getValue());
                  OrderedSequenceNumber<SequenceOffsetType> latestOffset = makeSequenceNumber(
                      latestOffsetsFromDb == null ? partitionOffset.getValue() :
                      latestOffsetsFromDb.getOrDefault(
                          partitionOffset
                              .getKey(),
                          partitionOffset
                              .getValue()
                      )
                  );

                  return sequence.compareTo(latestOffset) == 0;
                }
            ) && earliestConsistentSequenceId.compareAndSet(-1, sequenceCheckpoint.getKey())) || (
                pendingCompletionTaskGroups.getOrDefault(groupId, new CopyOnWriteArrayList<>()).size() > 0
                && earliestConsistentSequenceId.compareAndSet(-1, taskCheckpoints.firstKey()))) {
          final SortedMap<Integer, Map<PartitionIdType, SequenceOffsetType>> latestCheckpoints = new TreeMap<>(
              taskCheckpoints.tailMap(earliestConsistentSequenceId.get())
          );

          log.info("Setting taskGroup sequences to [%s] for group [%d]", latestCheckpoints, groupId);
          taskGroup.checkpointSequences.clear();
          taskGroup.checkpointSequences.putAll(latestCheckpoints);
        } else {
          log.debug(
              "Adding task [%s] to kill list, checkpoints[%s], latestoffsets from DB [%s]",
              taskId,
              taskCheckpoints,
              latestOffsetsFromDb
          );
          tasksToKill.add(taskId);
        }
      } else {
        // check consistency with taskGroup sequences
        if (taskCheckpoints.get(taskGroup.checkpointSequences.firstKey()) == null
            || !(taskCheckpoints.get(taskGroup.checkpointSequences.firstKey())
                                .equals(taskGroup.checkpointSequences.firstEntry().getValue()))
            || taskCheckpoints.tailMap(taskGroup.checkpointSequences.firstKey()).size()
               != taskGroup.checkpointSequences.size()) {
          log.debug(
              "Adding task [%s] to kill list, checkpoints[%s], taskgroup checkpoints [%s]",
              taskId,
              taskCheckpoints,
              taskGroup.checkpointSequences
          );
          tasksToKill.add(taskId);
        }
      }
      taskIndex++;
    }

    if ((tasksToKill.size() > 0 && tasksToKill.size() == taskGroup.tasks.size()) ||
        (taskGroup.tasks.size() == 0
         && pendingCompletionTaskGroups.getOrDefault(groupId, new CopyOnWriteArrayList<>()).size() == 0)) {
      // killing all tasks or no task left in the group ?
      // clear state about the taskgroup so that get latest sequence information is fetched from metadata store
      log.warn("Clearing task group [%d] information as no valid tasks left the group", groupId);
      activelyReadingTaskGroups.remove(groupId);
      for (PartitionIdType partitionId : taskGroup.startingSequences.keySet()) {
        partitionOffsets.put(partitionId, getNotSetMarker());
      }
    }

    taskSequences.stream().filter(taskIdSequences -> tasksToKill.contains(taskIdSequences.lhs)).forEach(
        sequenceCheckpoint -> {
          killTask(
              sequenceCheckpoint.lhs,
              "Killing task [%s], as its checkpoints [%s] are not consistent with group checkpoints[%s] or latest "
              + "persisted sequences in metadata store [%s]",
              sequenceCheckpoint.lhs,
              sequenceCheckpoint.rhs,
              taskGroup.checkpointSequences,
              latestOffsetsFromDb
          );
          taskGroup.tasks.remove(sequenceCheckpoint.lhs);
        }
    );
  }

  private void addDiscoveredTaskToPendingCompletionTaskGroups(
      int groupId,
      String taskId,
      Map<PartitionIdType, SequenceOffsetType> startingPartitions
  )
  {
    final CopyOnWriteArrayList<TaskGroup> taskGroupList = pendingCompletionTaskGroups.computeIfAbsent(
        groupId,
        k -> new CopyOnWriteArrayList<>()
    );
    for (TaskGroup taskGroup : taskGroupList) {
      if (taskGroup.startingSequences.equals(startingPartitions)) {
        if (taskGroup.tasks.putIfAbsent(taskId, new TaskData()) == null) {
          log.info("Added discovered task [%s] to existing pending task group [%s]", taskId, groupId);
        }
        return;
      }
    }

    log.info("Creating new pending completion task group [%s] for discovered task [%s]", groupId, taskId);

    // reading the minimumMessageTime & maximumMessageTime from the publishing task and setting it here is not necessary as this task cannot
    // change to a state where it will read any more events.
    // This is a discovered task, so it would not have been assigned closed partitions initially.
    TaskGroup newTaskGroup = new TaskGroup(
        groupId,
        ImmutableMap.copyOf(startingPartitions),
        null,
        Optional.absent(),
        Optional.absent(),
        null
    );

    newTaskGroup.tasks.put(taskId, new TaskData());
    newTaskGroup.completionTimeout = DateTimes.nowUtc().plus(ioConfig.getCompletionTimeout());

    taskGroupList.add(newTaskGroup);
  }

  // Sanity check to ensure that tasks have the same sequence name as their task group
  private void verifySameSequenceNameForAllTasksInGroup(int groupId)
  {
    String taskGroupSequenceName = activelyReadingTaskGroups.get(groupId).baseSequenceName;
    boolean allSequenceNamesMatch =
        activelyReadingTaskGroups.get(groupId)
            .tasks
            .keySet()
            .stream()
            .map(x -> {
              Optional<Task> taskOptional = taskStorage.getTask(x);
              if (!taskOptional.isPresent() || !doesTaskTypeMatchSupervisor(taskOptional.get())) {
                return false;
              }
              @SuppressWarnings("unchecked")
              SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType> task =
                  (SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType>) taskOptional.get();
              return task.getIOConfig().getBaseSequenceName();
            })
            .allMatch(taskSeqName -> taskSeqName.equals(taskGroupSequenceName));
    if (!allSequenceNamesMatch) {
      throw new ISE(
          "Base sequence names do not match for the tasks in the task group with ID [%s]",
          groupId
      );
    }
  }

  private ListenableFuture<Void> stopTask(final String id, final boolean publish)
  {
    return Futures.transform(
        taskClient.stopAsync(id, publish), new Function<Boolean, Void>()
        {
          @Nullable
          @Override
          public Void apply(@Nullable Boolean result)
          {
            if (result == null || !result) {
              log.info("Task [%s] failed to stop in a timely manner, killing task", id);
              killTask(id, "Task [%s] failed to stop in a timely manner, killing task", id);
            }
            return null;
          }
        }
    );
  }

  @VisibleForTesting
  public boolean isTaskCurrent(int taskGroupId, String taskId)
  {
    Optional<Task> taskOptional = taskStorage.getTask(taskId);
    if (!taskOptional.isPresent() || !doesTaskTypeMatchSupervisor(taskOptional.get())) {
      return false;
    }

    @SuppressWarnings("unchecked")
    SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType> task =
        (SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType>) taskOptional.get();

    // We recompute the sequence name hash for the supervisor's own configuration and compare this to the hash created
    // by rehashing the task's sequence name using the most up-to-date class definitions of tuning config and
    // data schema. Recomputing both ensures that forwards-compatible tasks won't be killed (which would occur
    // if the hash generated using the old class definitions was used).
    String taskSequenceName = generateSequenceName(
        task.getIOConfig().getStartSequenceNumbers().getPartitionSequenceNumberMap(),
        task.getIOConfig().getMinimumMessageTime(),
        task.getIOConfig().getMaximumMessageTime(),
        task.getDataSchema(),
        task.getTuningConfig()
    );

    if (activelyReadingTaskGroups.get(taskGroupId) != null) {
      TaskGroup taskGroup = activelyReadingTaskGroups.get(taskGroupId);
      return generateSequenceName(
          taskGroup.startingSequences,
          taskGroup.minimumMessageTime,
          taskGroup.maximumMessageTime,
          spec.getDataSchema(),
          taskTuningConfig
      ).equals(taskSequenceName);
    } else {
      return generateSequenceName(
          task.getIOConfig()
              .getStartSequenceNumbers()
              .getPartitionSequenceNumberMap(),
          task.getIOConfig().getMinimumMessageTime(),
          task.getIOConfig().getMaximumMessageTime(),
          spec.getDataSchema(),
          taskTuningConfig
      ).equals(taskSequenceName);
    }
  }

  @VisibleForTesting
  protected String generateSequenceName(
      Map<PartitionIdType, SequenceOffsetType> startPartitions,
      Optional<DateTime> minimumMessageTime,
      Optional<DateTime> maximumMessageTime,
      DataSchema dataSchema,
      SeekableStreamIndexTaskTuningConfig tuningConfig
  )
  {
    StringBuilder sb = new StringBuilder();

    for (Entry<PartitionIdType, SequenceOffsetType> entry : startPartitions.entrySet()) {
      sb.append(StringUtils.format("+%s(%s)", entry.getKey().toString(), entry.getValue().toString()));
    }
    String partitionOffsetStr = startPartitions.size() == 0 ? "" : sb.toString().substring(1);

    String minMsgTimeStr = (minimumMessageTime.isPresent() ? String.valueOf(minimumMessageTime.get().getMillis()) : "");
    String maxMsgTimeStr = (maximumMessageTime.isPresent() ? String.valueOf(maximumMessageTime.get().getMillis()) : "");

    String dataSchemaStr, tuningConfigStr;
    try {
      dataSchemaStr = sortingMapper.writeValueAsString(dataSchema);
      tuningConfigStr = sortingMapper.writeValueAsString(tuningConfig);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    String hashCode = DigestUtils.sha1Hex(dataSchemaStr
                                          + tuningConfigStr
                                          + partitionOffsetStr
                                          + minMsgTimeStr
                                          + maxMsgTimeStr)
                                 .substring(0, 15);

    return Joiner.on("_").join(baseTaskName(), dataSource, hashCode);
  }

  protected abstract String baseTaskName();

  protected boolean supportsPartitionExpiration()
  {
    return false;
  }

  private boolean updatePartitionDataFromStream()
  {
    List<PartitionIdType> previousPartitionIds = new ArrayList<>(partitionIds);
    Set<PartitionIdType> partitionIdsFromSupplier;
    recordSupplierLock.lock();
    try {
      partitionIdsFromSupplier = recordSupplier.getPartitionIds(ioConfig.getStream());
    }
    catch (Exception e) {
      stateManager.recordThrowableEvent(e);
      log.warn("Could not fetch partitions for topic/stream [%s]: %s", ioConfig.getStream(), e.getMessage());
      log.debug(e, "full stack trace");
      return false;
    }
    finally {
      recordSupplierLock.unlock();
    }

    if (partitionIdsFromSupplier == null || partitionIdsFromSupplier.size() == 0) {
      String errMsg = StringUtils.format("No partitions found for stream [%s]", ioConfig.getStream());
      stateManager.recordThrowableEvent(new StreamException(new ISE(errMsg)));
      log.warn(errMsg);
      return false;
    }

    log.debug("Found [%d] partitions for stream [%s]", partitionIdsFromSupplier.size(), ioConfig.getStream());

    Map<PartitionIdType, SequenceOffsetType> storedMetadata = getOffsetsFromMetadataStorage();
    Set<PartitionIdType> storedPartitions = storedMetadata.keySet();
    Set<PartitionIdType> closedPartitions = storedMetadata
        .entrySet()
        .stream()
        .filter(x -> isEndOfShard(x.getValue()))
        .map(Entry::getKey)
        .collect(Collectors.toSet());
    Set<PartitionIdType> previouslyExpiredPartitions = storedMetadata
        .entrySet()
        .stream()
        .filter(x -> isShardExpirationMarker(x.getValue()))
        .map(Entry::getKey)
        .collect(Collectors.toSet());

    Set<PartitionIdType> partitionIdsFromSupplierWithoutPreviouslyExpiredPartitions = Sets.difference(
        partitionIdsFromSupplier,
        previouslyExpiredPartitions
    );

    Set<PartitionIdType> activePartitionsIdsFromSupplier = Sets.difference(
        partitionIdsFromSupplierWithoutPreviouslyExpiredPartitions,
        closedPartitions
    );

    Set<PartitionIdType> newlyClosedPartitions = Sets.intersection(
        closedPartitions,
        new HashSet<>(previousPartitionIds)
    );

    log.debug("active partitions from supplier: " + activePartitionsIdsFromSupplier);

    if (partitionIdsFromSupplierWithoutPreviouslyExpiredPartitions.size() != partitionIdsFromSupplier.size()) {
      // this should never happen, but we check for it and exclude the expired partitions if they somehow reappear
      log.warn(
          "Previously expired partitions [%s] were present in the current list [%s] from the record supplier.",
          previouslyExpiredPartitions,
          partitionIdsFromSupplier
      );
    }
    if (activePartitionsIdsFromSupplier.size() == 0) {
      String errMsg = StringUtils.format(
          "No active partitions found for stream [%s] after removing closed and previously expired partitions",
          ioConfig.getStream()
      );
      stateManager.recordThrowableEvent(new StreamException(new ISE(errMsg)));
      log.warn(errMsg);
      return false;
    }

    boolean initialPartitionDiscovery = this.partitionIds.isEmpty();
    for (PartitionIdType partitionId : partitionIdsFromSupplierWithoutPreviouslyExpiredPartitions) {
      if (closedPartitions.contains(partitionId)) {
        log.info("partition [%s] is closed and has no more data, skipping.", partitionId);
        continue;
      }

      if (!this.partitionIds.contains(partitionId)) {
        partitionIds.add(partitionId);

        if (!initialPartitionDiscovery) {
          subsequentlyDiscoveredPartitions.add(partitionId);
        }
      }
    }

    // When partitions expire, we need to recompute the task group assignments, considering only
    // non-closed and non-expired partitions, to ensure that we have even distribution of active
    // partitions across tasks.
    if (supportsPartitionExpiration()) {
      cleanupClosedAndExpiredPartitions(
          storedPartitions,
          newlyClosedPartitions,
          activePartitionsIdsFromSupplier,
          previouslyExpiredPartitions,
          partitionIdsFromSupplier
      );
    }

    Int2ObjectMap<List<PartitionIdType>> newlyDiscovered = new Int2ObjectLinkedOpenHashMap<>();
    for (PartitionIdType partitionId : activePartitionsIdsFromSupplier) {
      int taskGroupId = getTaskGroupIdForPartition(partitionId);
      Set<PartitionIdType> partitionGroup = partitionGroups.computeIfAbsent(
          taskGroupId,
          k -> new HashSet<>()
      );
      partitionGroup.add(partitionId);

      if (partitionOffsets.putIfAbsent(partitionId, getNotSetMarker()) == null) {
        log.debug(
            "New partition [%s] discovered for stream [%s], added to task group [%d]",
            partitionId,
            ioConfig.getStream(),
            taskGroupId
        );

        newlyDiscovered.computeIfAbsent(taskGroupId, ArrayList::new).add(partitionId);
      }
    }

    if (newlyDiscovered.size() > 0) {
      for (Int2ObjectMap.Entry<List<PartitionIdType>> taskGroupPartitions : newlyDiscovered.int2ObjectEntrySet()) {
        log.info(
            "New partitions %s discovered for stream [%s], added to task group [%s]",
            taskGroupPartitions.getValue(),
            ioConfig.getStream(),
            taskGroupPartitions.getIntKey()
        );
      }
    }

    if (!partitionIds.equals(previousPartitionIds)) {
      assignRecordSupplierToPartitionIds();
      // the set of partition IDs has changed, have any running tasks stop early so that we can adjust to the
      // repartitioning quickly by creating new tasks
      for (TaskGroup taskGroup : activelyReadingTaskGroups.values()) {
        if (!taskGroup.taskIds().isEmpty()) {
          // Partitions have changed and we are managing active tasks - set an early publish time
          // at the current time + repartitionTransitionDuration.
          // This allows time for the stream to start writing to the new partitions after repartitioning.
          // For Kinesis ingestion, this cooldown time is particularly useful, lowering the possibility of
          // the new shards being empty, which can cause issues presently
          // (see https://github.com/apache/druid/issues/7600)
          earlyStopTime = DateTimes.nowUtc().plus(tuningConfig.getRepartitionTransitionDuration());
          log.info(
              "Previous partition set [%s] has changed to [%s] - requesting that tasks stop after [%s] at [%s]",
              previousPartitionIds,
              partitionIds,
              tuningConfig.getRepartitionTransitionDuration(),
              earlyStopTime
          );
          break;
        }
      }
    }

    return true;
  }

  private void assignRecordSupplierToPartitionIds()
  {
    recordSupplierLock.lock();
    try {
      final Set partitions = partitionIds.stream()
                                         .map(partitionId -> new StreamPartition<>(ioConfig.getStream(), partitionId))
                                         .collect(Collectors.toSet());
      if (!recordSupplier.getAssignment().containsAll(partitions)) {
        recordSupplier.assign(partitions);
        try {
          recordSupplier.seekToEarliest(partitions);
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    finally {
      recordSupplierLock.unlock();
    }
  }

  /**
   * This method determines the set of expired partitions from the set of partitions currently returned by
   * the record supplier and the set of partitions previously tracked in the metadata.
   * <p>
   * It will mark the expired partitions in metadata and recompute the partition->task group mappings, updating
   * the metadata, the partitionIds list, and the partitionGroups mappings.
   *
   * @param storedPartitions                Set of partitions previously tracked, from the metadata store
   * @param newlyClosedPartitions           Set of partitions that are closed in the metadata store but still present in the
   *                                        current {@link #partitionIds}
   * @param activePartitionsIdsFromSupplier Set of partitions currently returned by the record supplier, but with
   *                                        any partitions that are closed/expired in the metadata store removed
   * @param previouslyExpiredPartitions     Set of partitions that are recorded as expired in the metadata store
   * @param partitionIdsFromSupplier        Set of partitions currently returned by the record supplier.
   */
  private void cleanupClosedAndExpiredPartitions(
      Set<PartitionIdType> storedPartitions,
      Set<PartitionIdType> newlyClosedPartitions,
      Set<PartitionIdType> activePartitionsIdsFromSupplier,
      Set<PartitionIdType> previouslyExpiredPartitions,
      Set<PartitionIdType> partitionIdsFromSupplier
  )
  {
    // If a partition was previously known (stored in metadata) but no longer appears in the list of partitions
    // provided by the record supplier, it has expired.
    Set<PartitionIdType> newlyExpiredPartitions = Sets.difference(storedPartitions, previouslyExpiredPartitions);
    newlyExpiredPartitions = Sets.difference(newlyExpiredPartitions, partitionIdsFromSupplier);

    if (!newlyExpiredPartitions.isEmpty()) {
      log.info("Detected newly expired partitions: " + newlyExpiredPartitions);

      // Mark partitions as expired in metadata
      @SuppressWarnings("unchecked")
      SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> currentMetadata =
          (SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType>)
              indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(dataSource);

      SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> cleanedMetadata =
          createDataSourceMetadataWithExpiredPartitions(currentMetadata, newlyExpiredPartitions);

      log.info("New metadata after partition expiration: " + cleanedMetadata);

      validateMetadataPartitionExpiration(newlyExpiredPartitions, currentMetadata, cleanedMetadata);

      try {
        boolean success = indexerMetadataStorageCoordinator.resetDataSourceMetadata(dataSource, cleanedMetadata);
        if (!success) {
          log.error("Failed to update datasource metadata[%s] with expired partitions removed", cleanedMetadata);
        }
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    if (!newlyClosedPartitions.isEmpty()) {
      log.info("Detected newly closed partitions: " + newlyClosedPartitions);
    }

    // Partitions have been dropped
    if (!newlyClosedPartitions.isEmpty() || !newlyExpiredPartitions.isEmpty()) {
      // Compute new partition groups, only including partitions that are
      // still in partitionIdsFromSupplier and not closed
      Map<Integer, Set<PartitionIdType>> newPartitionGroups =
          recomputePartitionGroupsForExpiration(activePartitionsIdsFromSupplier);

      validatePartitionGroupReassignments(activePartitionsIdsFromSupplier, newPartitionGroups);

      log.info("New partition groups after removing closed and expired partitions: " + newPartitionGroups);

      partitionIds.clear();
      partitionIds.addAll(activePartitionsIdsFromSupplier);
      assignRecordSupplierToPartitionIds();

      for (Integer groupId : partitionGroups.keySet()) {
        if (newPartitionGroups.containsKey(groupId)) {
          partitionGroups.put(groupId, newPartitionGroups.get(groupId));
        } else {
          partitionGroups.put(groupId, new HashSet<>());
        }
      }
    }
  }

  /**
   * When partitions are removed due to expiration it may be necessary to recompute the partitionID -> groupID
   * mappings to ensure balanced distribution of partitions.
   * <p>
   * This function should return a copy of partitionGroups, using the provided availablePartitions as the list of
   * active partitions, reassigning partitions to different groups if necessary.
   * <p>
   * If a partition is not in availablePartitions, it should be filtered out of the new partition groups returned
   * by this method.
   *
   * @param availablePartitions
   *
   * @return a remapped copy of partitionGroups, containing only the partitions in availablePartitions
   */
  protected Map<Integer, Set<PartitionIdType>> recomputePartitionGroupsForExpiration(
      Set<PartitionIdType> availablePartitions
  )
  {
    throw new UnsupportedOperationException("This supervisor type does not support partition expiration.");
  }

  /**
   * Some seekable stream systems such as Kinesis allow partitions to expire. When this occurs, the supervisor should
   * mark the expired partitions in the saved metadata. This method returns a copy of the current metadata
   * with any expired partitions marked with an implementation-specific offset value that represents the expired state.
   *
   * @param currentMetadata     The current DataSourceMetadata from metadata storage
   * @param expiredPartitionIds The set of expired partition IDs.
   *
   * @return currentMetadata but with any expired partitions removed.
   */
  protected SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> createDataSourceMetadataWithExpiredPartitions(
      SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> currentMetadata,
      Set<PartitionIdType> expiredPartitionIds
  )
  {
    throw new UnsupportedOperationException("This supervisor type does not support partition expiration.");
  }

  /**
   * Perform a sanity check on the datasource metadata returned by
   * {@link #createDataSourceMetadataWithExpiredPartitions}.
   * <p>
   * Specifically, we check that the cleaned metadata's partitions are a subset of the original metadata's partitions,
   * that newly expired partitions are marked as expired, and that none of the offsets for the non-expired partitions
   * have changed.
   *
   * @param oldMetadata     metadata containing expired partitions.
   * @param cleanedMetadata new metadata without expired partitions, generated by the subclass
   */
  private void validateMetadataPartitionExpiration(
      Set<PartitionIdType> newlyExpiredPartitions,
      SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> oldMetadata,
      SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> cleanedMetadata
  )
  {
    Map<PartitionIdType, SequenceOffsetType> oldPartitionSeqNos = oldMetadata.getSeekableStreamSequenceNumbers()
                                                                             .getPartitionSequenceNumberMap();

    Map<PartitionIdType, SequenceOffsetType> cleanedPartitionSeqNos = cleanedMetadata.getSeekableStreamSequenceNumbers()
                                                                                     .getPartitionSequenceNumberMap();

    for (Entry<PartitionIdType, SequenceOffsetType> cleanedPartitionSeqNo : cleanedPartitionSeqNos.entrySet()) {
      if (!oldPartitionSeqNos.containsKey(cleanedPartitionSeqNo.getKey())) {
        // cleaning the expired partitions added a partition somehow
        throw new IAE(
            "Cleaned partition map [%s] contains unexpected partition ID [%s], original partition map: [%s]",
            cleanedPartitionSeqNos,
            cleanedPartitionSeqNo.getKey(),
            oldPartitionSeqNos
        );
      }

      SequenceOffsetType oldOffset = oldPartitionSeqNos.get(cleanedPartitionSeqNo.getKey());
      if (newlyExpiredPartitions.contains(cleanedPartitionSeqNo.getKey())) {
        // this is a newly expired partition, check that we did actually mark it as expired
        if (!isShardExpirationMarker(cleanedPartitionSeqNo.getValue())) {
          throw new IAE(
              "Newly expired partition [%] was not marked as expired in the cleaned partition map [%s], original partition map: [%s]",
              cleanedPartitionSeqNo.getKey(),
              cleanedPartitionSeqNos,
              oldPartitionSeqNos
          );
        }
      } else if (!oldOffset.equals(cleanedPartitionSeqNo.getValue())) {
        // this is not an expired shard, check that the offset did not change
        throw new IAE(
            "Cleaned partition map [%s] has offset mismatch for partition ID [%s], original partition map: [%s]",
            cleanedPartitionSeqNos,
            cleanedPartitionSeqNo.getKey(),
            oldPartitionSeqNos
        );
      }
    }
  }

  /**
   * Perform a sanity check on the new partition groups returned by
   * {@link #recomputePartitionGroupsForExpiration}.
   * <p>
   * Specifically, we check that the new partition groups' partitions are a subset of the original groups' partitions,
   * and that none of the offsets for the non-expired partitions have changed.
   *
   * @param newPartitionGroups new metadata without expired partitions, generated by the subclass
   */
  private void validatePartitionGroupReassignments(
      Set<PartitionIdType> activePartitionsIdsFromSupplier,
      Map<Integer, Set<PartitionIdType>> newPartitionGroups
  )
  {
    for (Set<PartitionIdType> newGroup : newPartitionGroups.values()) {
      for (PartitionIdType partitionInNewGroup : newGroup) {
        if (!activePartitionsIdsFromSupplier.contains(partitionInNewGroup)) {
          // recomputing the groups without the expired partitions added an unknown partition somehow
          throw new IAE(
              "Recomputed partition groups [%s] contains unexpected partition ID [%s], old partition groups: [%s]",
              newPartitionGroups,
              partitionInNewGroup,
              partitionGroups
          );
        }
      }
    }
  }

  private void updateTaskStatus() throws ExecutionException, InterruptedException, TimeoutException
  {
    final List<ListenableFuture<Boolean>> futures = new ArrayList<>();
    final List<String> futureTaskIds = new ArrayList<>();

    // update status (and startTime if unknown) of current tasks in activelyReadingTaskGroups
    for (TaskGroup group : activelyReadingTaskGroups.values()) {
      for (Entry<String, TaskData> entry : group.tasks.entrySet()) {
        final String taskId = entry.getKey();
        final TaskData taskData = entry.getValue();

        if (taskData.startTime == null) {
          futureTaskIds.add(taskId);
          futures.add(
              Futures.transform(
                  taskClient.getStartTimeAsync(taskId), new Function<DateTime, Boolean>()
                  {
                    @Override
                    public Boolean apply(@Nullable DateTime startTime)
                    {
                      if (startTime == null) {
                        return false;
                      }

                      taskData.startTime = startTime;
                      long millisRemaining = ioConfig.getTaskDuration().getMillis() -
                                             (System.currentTimeMillis() - taskData.startTime.getMillis());
                      if (millisRemaining > 0) {
                        scheduledExec.schedule(
                            buildRunTask(),
                            millisRemaining + MAX_RUN_FREQUENCY_MILLIS,
                            TimeUnit.MILLISECONDS
                        );
                      }

                      return true;
                    }
                  }, workerExec
              )
          );
        }

        taskData.status = taskStorage.getStatus(taskId).get();
      }
    }

    // update status of pending completion tasks in pendingCompletionTaskGroups
    for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
      for (TaskGroup group : taskGroups) {
        for (Entry<String, TaskData> entry : group.tasks.entrySet()) {
          entry.getValue().status = taskStorage.getStatus(entry.getKey()).get();
        }
      }
    }

    List<Boolean> results = Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int i = 0; i < results.size(); i++) {
      // false means the task hasn't started running yet and that's okay; null means it should be running but the HTTP
      // request threw an exception so kill the task
      if (results.get(i) == null) {
        String taskId = futureTaskIds.get(i);
        log.warn("Task [%s] failed to return start time, killing task", taskId);
        killTask(taskId, "Task [%s] failed to return start time, killing task", taskId);
      }
    }
  }

  private void checkTaskDuration() throws ExecutionException, InterruptedException, TimeoutException
  {
    final List<ListenableFuture<Map<PartitionIdType, SequenceOffsetType>>> futures = new ArrayList<>();
    final List<Integer> futureGroupIds = new ArrayList<>();

    for (Entry<Integer, TaskGroup> entry : activelyReadingTaskGroups.entrySet()) {
      Integer groupId = entry.getKey();
      TaskGroup group = entry.getValue();

      // find the longest running task from this group
      DateTime earliestTaskStart = DateTimes.nowUtc();
      for (TaskData taskData : group.tasks.values()) {
        if (taskData.startTime != null && earliestTaskStart.isAfter(taskData.startTime)) {
          earliestTaskStart = taskData.startTime;
        }
      }


      boolean stopTasksEarly = false;
      if (earlyStopTime != null && (earlyStopTime.isBeforeNow() || earlyStopTime.isEqualNow())) {
        log.info("Early stop requested - signalling tasks to complete");

        earlyStopTime = null;
        stopTasksEarly = true;
      }


      // if this task has run longer than the configured duration, signal all tasks in the group to persist
      if (earliestTaskStart.plus(ioConfig.getTaskDuration()).isBeforeNow() || stopTasksEarly) {
        log.info("Task group [%d] has run for [%s]", groupId, ioConfig.getTaskDuration());
        futureGroupIds.add(groupId);
        futures.add(checkpointTaskGroup(group, true));
      }
    }

    List<Map<PartitionIdType, SequenceOffsetType>> results = Futures.successfulAsList(futures)
                                                                    .get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int j = 0; j < results.size(); j++) {
      Integer groupId = futureGroupIds.get(j);
      TaskGroup group = activelyReadingTaskGroups.get(groupId);
      Map<PartitionIdType, SequenceOffsetType> endOffsets = results.get(j);

      if (endOffsets != null) {
        // set a timeout and put this group in pendingCompletionTaskGroups so that it can be monitored for completion
        group.completionTimeout = DateTimes.nowUtc().plus(ioConfig.getCompletionTimeout());
        pendingCompletionTaskGroups.computeIfAbsent(groupId, k -> new CopyOnWriteArrayList<>()).add(group);


        boolean endOffsetsAreInvalid = false;
        for (Entry<PartitionIdType, SequenceOffsetType> entry : endOffsets.entrySet()) {
          if (entry.getValue().equals(getEndOfPartitionMarker())) {
            log.info(
                "Got end of partition marker for partition [%s] in checkTaskDuration, not updating partition offset.",
                entry.getKey()
            );
            endOffsetsAreInvalid = true;
          }
        }

        // set endOffsets as the next startOffsets
        // If we received invalid endOffset values, we clear the known offset to refetch the last committed offset
        // from metadata. If any endOffset values are invalid, we treat the entire set as invalid as a safety measure.
        if (!endOffsetsAreInvalid) {
          for (Entry<PartitionIdType, SequenceOffsetType> entry : endOffsets.entrySet()) {
            partitionOffsets.put(entry.getKey(), entry.getValue());
          }
        } else {
          for (Entry<PartitionIdType, SequenceOffsetType> entry : endOffsets.entrySet()) {
            partitionOffsets.put(entry.getKey(), getNotSetMarker());
          }
        }
      } else {
        for (String id : group.taskIds()) {
          killTask(
              id,
              "All tasks in group [%s] failed to transition to publishing state",
              groupId
          );
        }
        // clear partitionGroups, so that latest sequences from db is used as start sequences not the stale ones
        // if tasks did some successful incremental handoffs
        for (PartitionIdType partitionId : group.startingSequences.keySet()) {
          partitionOffsets.put(partitionId, getNotSetMarker());
        }
      }

      // remove this task group from the list of current task groups now that it has been handled
      activelyReadingTaskGroups.remove(groupId);
    }
  }

  private ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> checkpointTaskGroup(
      final TaskGroup taskGroup,
      final boolean finalize
  )
  {
    if (finalize) {
      // 1) Check if any task completed (in which case we're done) and kill unassigned tasks
      Iterator<Entry<String, TaskData>> i = taskGroup.tasks.entrySet().iterator();
      while (i.hasNext()) {
        Entry<String, TaskData> taskEntry = i.next();
        String taskId = taskEntry.getKey();
        TaskData task = taskEntry.getValue();

        if (task.status != null) {
          if (task.status.isSuccess()) {
            // If any task in this group has already completed, stop the rest of the tasks in the group and return.
            // This will cause us to create a new set of tasks next cycle that will start from the sequences in
            // metadata store (which will have advanced if we succeeded in publishing and will remain the same if
            // publishing failed and we need to re-ingest)
            stateManager.recordCompletedTaskState(TaskState.SUCCESS);
            return Futures.transform(
                stopTasksInGroup(taskGroup, "task[%s] succeeded in the taskGroup", task.status.getId()),
                new Function<Object, Map<PartitionIdType, SequenceOffsetType>>()
                {
                  @Nullable
                  @Override
                  public Map<PartitionIdType, SequenceOffsetType> apply(@Nullable Object input)
                  {
                    return null;
                  }
                }
            );
          }

          if (task.status.isRunnable()) {
            if (taskInfoProvider.getTaskLocation(taskId).equals(TaskLocation.unknown())) {
              killTask(taskId, "Killing task [%s] which hasn't been assigned to a worker", taskId);
              i.remove();
            }
          }
        }
      }
    }

    // 2) Pause running tasks
    final List<ListenableFuture<Map<PartitionIdType, SequenceOffsetType>>> pauseFutures = new ArrayList<>();
    final List<String> pauseTaskIds = ImmutableList.copyOf(taskGroup.taskIds());
    for (final String taskId : pauseTaskIds) {
      pauseFutures.add(taskClient.pauseAsync(taskId));
    }

    return Futures.transform(
        Futures.successfulAsList(pauseFutures),
        new Function<List<Map<PartitionIdType, SequenceOffsetType>>, Map<PartitionIdType, SequenceOffsetType>>()
        {
          @Nullable
          @Override
          public Map<PartitionIdType, SequenceOffsetType> apply(List<Map<PartitionIdType, SequenceOffsetType>> input)
          {
            // 3) Build a map of the highest sequence read by any task in the group for each partition
            final Map<PartitionIdType, SequenceOffsetType> endOffsets = new HashMap<>();
            for (int i = 0; i < input.size(); i++) {
              final Map<PartitionIdType, SequenceOffsetType> result = input.get(i);
              final String taskId = pauseTaskIds.get(i);

              if (result == null) {
                // Get the exception
                final Throwable pauseException;
                try {
                  // The below get should throw ExecutionException since result is null.
                  final Map<PartitionIdType, SequenceOffsetType> pauseResult = pauseFutures.get(i).get();
                  throw new ISE(
                      "Pause request for task [%s] should have failed, but returned [%s]",
                      taskId,
                      pauseResult
                  );
                }
                catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                catch (ExecutionException e) {
                  stateManager.recordThrowableEvent(e);
                  pauseException = e.getCause() == null ? e : e.getCause();
                }

                killTask(
                    taskId,
                    "An exception occured while waiting for task [%s] to pause: [%s]",
                    taskId,
                    pauseException
                );
                taskGroup.tasks.remove(taskId);

              } else if (result.isEmpty()) {
                killTask(taskId, "Task [%s] returned empty offsets after pause", taskId);
                taskGroup.tasks.remove(taskId);
              } else { // otherwise build a map of the highest sequences seen
                for (Entry<PartitionIdType, SequenceOffsetType> sequence : result.entrySet()) {
                  if (!endOffsets.containsKey(sequence.getKey())
                      || makeSequenceNumber(endOffsets.get(sequence.getKey())).compareTo(
                      makeSequenceNumber(sequence.getValue())) < 0) {
                    endOffsets.put(sequence.getKey(), sequence.getValue());
                  }
                }
              }
            }

            // 4) Set the end sequences for each task to the values from step 3 and resume the tasks. All the tasks should
            //    finish reading and start publishing within a short period, depending on how in sync the tasks were.
            final List<ListenableFuture<Boolean>> setEndOffsetFutures = new ArrayList<>();
            final List<String> setEndOffsetTaskIds = ImmutableList.copyOf(taskGroup.taskIds());

            if (setEndOffsetTaskIds.isEmpty()) {
              log.info("All tasks in taskGroup [%d] have failed, tasks will be re-created", taskGroup.groupId);
              return null;
            }

            try {

              if (endOffsets.equals(taskGroup.checkpointSequences.lastEntry().getValue())) {
                log.warn(
                    "Checkpoint [%s] is same as the start sequences [%s] of latest sequence for the task group [%d]",
                    endOffsets,
                    taskGroup.checkpointSequences.lastEntry().getValue(),
                    taskGroup.groupId
                );
              }

              log.info(
                  "Setting endOffsets for tasks in taskGroup [%d] to %s and resuming",
                  taskGroup.groupId,
                  endOffsets
              );
              for (final String taskId : setEndOffsetTaskIds) {
                setEndOffsetFutures.add(taskClient.setEndOffsetsAsync(taskId, endOffsets, finalize));
              }

              List<Boolean> results = Futures.successfulAsList(setEndOffsetFutures)
                                             .get(futureTimeoutInSeconds, TimeUnit.SECONDS);
              for (int i = 0; i < results.size(); i++) {
                if (results.get(i) == null || !results.get(i)) {
                  String taskId = setEndOffsetTaskIds.get(i);
                  killTask(
                      taskId,
                      "Task [%s] failed to respond to [set end offsets] in a timely manner, killing task",
                      taskId
                  );
                  taskGroup.tasks.remove(taskId);
                }
              }
            }
            catch (Exception e) {
              log.error("Something bad happened [%s]", e.getMessage());
              throw new RuntimeException(e);
            }

            if (taskGroup.tasks.isEmpty()) {
              log.info("All tasks in taskGroup [%d] have failed, tasks will be re-created", taskGroup.groupId);
              return null;
            }

            return endOffsets;
          }
        },
        workerExec
    );
  }

  private ListenableFuture<?> stopTasksInGroup(@Nullable TaskGroup taskGroup, String stopReasonFormat, Object... args)
  {
    if (taskGroup == null) {
      return Futures.immediateFuture(null);
    }

    log.info(
        "Stopping all tasks in taskGroup[%s] because: [%s]",
        taskGroup.groupId,
        StringUtils.format(stopReasonFormat, args)
    );

    final List<ListenableFuture<Void>> futures = new ArrayList<>();
    for (Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
      final String taskId = entry.getKey();
      final TaskData taskData = entry.getValue();
      if (taskData.status == null) {
        killTask(taskId, "Killing task since task status is not known to supervisor");
      } else if (!taskData.status.isComplete()) {
        futures.add(stopTask(taskId, false));
      }
    }

    return Futures.successfulAsList(futures);
  }

  private void checkPendingCompletionTasks()
      throws ExecutionException, InterruptedException, TimeoutException
  {
    List<ListenableFuture<?>> futures = new ArrayList<>();

    for (Entry<Integer, CopyOnWriteArrayList<TaskGroup>> pendingGroupList : pendingCompletionTaskGroups.entrySet()) {

      boolean stopTasksInTaskGroup = false;
      Integer groupId = pendingGroupList.getKey();
      CopyOnWriteArrayList<TaskGroup> taskGroupList = pendingGroupList.getValue();
      List<TaskGroup> toRemove = new ArrayList<>();

      for (TaskGroup group : taskGroupList) {
        boolean foundSuccess = false, entireTaskGroupFailed = false;

        if (stopTasksInTaskGroup) {
          // One of the earlier groups that was handling the same partition set timed out before the segments were
          // published so stop any additional groups handling the same partition set that are pending completion.
          futures.add(
              stopTasksInGroup(
                  group,
                  "one of earlier groups that was handling the same partition set timed out before publishing segments"
              )
          );
          toRemove.add(group);
          continue;
        }

        Iterator<Entry<String, TaskData>> iTask = group.tasks.entrySet().iterator();
        while (iTask.hasNext()) {
          final Entry<String, TaskData> entry = iTask.next();
          final String taskId = entry.getKey();
          final TaskData taskData = entry.getValue();

          Preconditions.checkNotNull(taskData.status, "task[%s] has null status", taskId);

          if (taskData.status.isFailure()) {
            stateManager.recordCompletedTaskState(TaskState.FAILED);
            iTask.remove(); // remove failed task
            if (group.tasks.isEmpty()) {
              // if all tasks in the group have failed, just nuke all task groups with this partition set and restart
              entireTaskGroupFailed = true;
              break;
            }
          }

          if (taskData.status.isSuccess()) {
            // If one of the pending completion tasks was successful, stop the rest of the tasks in the group as
            // we no longer need them to publish their segment.
            log.info("Task [%s] completed successfully, stopping tasks %s", taskId, group.taskIds());
            stateManager.recordCompletedTaskState(TaskState.SUCCESS);
            futures.add(
                stopTasksInGroup(group, "Task [%s] completed successfully, stopping tasks %s", taskId, group.taskIds())
            );
            foundSuccess = true;
            toRemove.add(group); // remove the TaskGroup from the list of pending completion task groups
            break; // skip iterating the rest of the tasks in this group as they've all been stopped now
          }
        }

        if ((!foundSuccess && group.completionTimeout.isBeforeNow()) || entireTaskGroupFailed) {
          if (entireTaskGroupFailed) {
            log.warn("All tasks in group [%d] failed to publish, killing all tasks for these partitions", groupId);
          } else {
            log.makeAlert(
                "No task in [%s] for taskGroup [%d] succeeded before the completion timeout elapsed [%s]!",
                group.taskIds(),
                groupId,
                ioConfig.getCompletionTimeout()
            ).emit();
          }

          // reset partitions sequences for this task group so that they will be re-read from metadata storage
          for (PartitionIdType partitionId : group.startingSequences.keySet()) {
            partitionOffsets.put(partitionId, getNotSetMarker());
          }

          // kill all the tasks in this pending completion group
          killTasksInGroup(
              group,
              "No task in pending completion taskGroup[%d] succeeded before completion timeout elapsed",
              groupId
          );
          // set a flag so the other pending completion groups for this set of partitions will also stop
          stopTasksInTaskGroup = true;

          // kill all the tasks in the currently reading task group and remove the bad task group
          killTasksInGroup(
              activelyReadingTaskGroups.remove(groupId),
              "No task in the corresponding pending completion taskGroup[%d] succeeded before completion timeout elapsed",
              groupId
          );
          toRemove.add(group);
        }
      }

      taskGroupList.removeAll(toRemove);
    }

    // wait for all task shutdowns to complete before returning
    Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
  }

  private void checkCurrentTaskState() throws ExecutionException, InterruptedException, TimeoutException
  {
    List<ListenableFuture<?>> futures = new ArrayList<>();
    Iterator<Entry<Integer, TaskGroup>> iTaskGroups = activelyReadingTaskGroups.entrySet().iterator();
    while (iTaskGroups.hasNext()) {
      Entry<Integer, TaskGroup> taskGroupEntry = iTaskGroups.next();
      Integer groupId = taskGroupEntry.getKey();
      TaskGroup taskGroup = taskGroupEntry.getValue();

      // Iterate the list of known tasks in this group and:
      //   1) Kill any tasks which are not "current" (have the partitions, starting sequences, and minimumMessageTime
      //      & maximumMessageTime (if applicable) in [activelyReadingTaskGroups])
      //   2) Remove any tasks that have failed from the list
      //   3) If any task completed successfully, stop all the tasks in this group and move to the next group

      log.debug("Task group [%d] pre-pruning: %s", groupId, taskGroup.taskIds());

      Iterator<Entry<String, TaskData>> iTasks = taskGroup.tasks.entrySet().iterator();
      while (iTasks.hasNext()) {
        Entry<String, TaskData> task = iTasks.next();
        String taskId = task.getKey();
        TaskData taskData = task.getValue();

        // stop and remove bad tasks from the task group
        if (!isTaskCurrent(groupId, taskId)) {
          log.info("Stopping task [%s] which does not match the expected sequence range and ingestion spec", taskId);
          futures.add(stopTask(taskId, false));
          iTasks.remove();
          continue;
        }

        Preconditions.checkNotNull(taskData.status, "Task[%s] has null status", taskId);

        // remove failed tasks
        if (taskData.status.isFailure()) {
          stateManager.recordCompletedTaskState(TaskState.FAILED);
          iTasks.remove();
          continue;
        }

        // check for successful tasks, and if we find one, stop all tasks in the group and remove the group so it can
        // be recreated with the next set of sequences
        if (taskData.status.isSuccess()) {
          stateManager.recordCompletedTaskState(TaskState.SUCCESS);
          futures.add(stopTasksInGroup(taskGroup, "task[%s] succeeded in the same taskGroup", taskData.status.getId()));
          iTaskGroups.remove();
          break;
        }
      }
      log.debug("Task group [%d] post-pruning: %s", groupId, taskGroup.taskIds());
    }

    // wait for all task shutdowns to complete before returning
    Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
  }

  /**
   * If the seekable stream system supported by this supervisor allows for partition expiration, expired partitions
   * should be removed from the starting offsets sent to the tasks.
   *
   * @param startingOffsets
   *
   * @return startingOffsets with entries for expired partitions removed
   */
  protected Map<PartitionIdType, OrderedSequenceNumber<SequenceOffsetType>> filterExpiredPartitionsFromStartingOffsets(
      Map<PartitionIdType, OrderedSequenceNumber<SequenceOffsetType>> startingOffsets
  )
  {
    return startingOffsets;
  }

  private void createNewTasks() throws JsonProcessingException
  {
    // update the checkpoints in the taskGroup to latest ones so that new tasks do not read what is already published
    verifyAndMergeCheckpoints(
        activelyReadingTaskGroups.values()
                                 .stream()
                                 .filter(taskGroup -> taskGroup.tasks.size() < ioConfig.getReplicas())
                                 .collect(Collectors.toList())
    );

    // check that there is a current task group for each group of partitions in [partitionGroups]
    for (Integer groupId : partitionGroups.keySet()) {
      if (!activelyReadingTaskGroups.containsKey(groupId)) {
        log.info("Creating new task group [%d] for partitions %s", groupId, partitionGroups.get(groupId));
        Optional<DateTime> minimumMessageTime;
        if (ioConfig.getLateMessageRejectionStartDateTime().isPresent()) {
          minimumMessageTime = Optional.of(ioConfig.getLateMessageRejectionStartDateTime().get());
        } else {
          minimumMessageTime = (ioConfig.getLateMessageRejectionPeriod().isPresent() ? Optional.of(
              DateTimes.nowUtc().minus(ioConfig.getLateMessageRejectionPeriod().get())
          ) : Optional.absent());
        }

        Optional<DateTime> maximumMessageTime = (ioConfig.getEarlyMessageRejectionPeriod().isPresent() ? Optional.of(
            DateTimes.nowUtc().plus(ioConfig.getTaskDuration()).plus(ioConfig.getEarlyMessageRejectionPeriod().get())
        ) : Optional.absent());

        final Map<PartitionIdType, OrderedSequenceNumber<SequenceOffsetType>> unfilteredStartingOffsets =
            generateStartingSequencesForPartitionGroup(groupId);

        final Map<PartitionIdType, OrderedSequenceNumber<SequenceOffsetType>> startingOffsets;
        if (supportsPartitionExpiration()) {
          startingOffsets = filterExpiredPartitionsFromStartingOffsets(unfilteredStartingOffsets);
        } else {
          startingOffsets = unfilteredStartingOffsets;
        }

        ImmutableMap<PartitionIdType, SequenceOffsetType> simpleStartingOffsets = startingOffsets
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().get() != null)
            .collect(
                Collectors.collectingAndThen(
                    Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()),
                    ImmutableMap::copyOf
                )
            );

        ImmutableMap<PartitionIdType, SequenceOffsetType> simpleUnfilteredStartingOffsets;
        if (supportsPartitionExpiration()) {
          simpleUnfilteredStartingOffsets = unfilteredStartingOffsets
              .entrySet()
              .stream()
              .filter(entry -> entry.getValue().get() != null)
              .collect(
                  Collectors.collectingAndThen(
                      Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()),
                      ImmutableMap::copyOf
                  )
              );
        } else {
          simpleUnfilteredStartingOffsets = simpleStartingOffsets;
        }

        Set<PartitionIdType> exclusiveStartSequenceNumberPartitions;
        if (!useExclusiveStartingSequence) {
          exclusiveStartSequenceNumberPartitions = Collections.emptySet();
        } else {
          exclusiveStartSequenceNumberPartitions = startingOffsets
              .entrySet()
              .stream()
              .filter(x -> x.getValue().get() != null
                           && x.getValue().isExclusive())
              .map(Entry::getKey)
              .collect(Collectors.toSet());
        }

        activelyReadingTaskGroups.put(
            groupId,
            new TaskGroup(
                groupId,
                simpleStartingOffsets,
                simpleUnfilteredStartingOffsets,
                minimumMessageTime,
                maximumMessageTime,
                exclusiveStartSequenceNumberPartitions
            )
        );
      }
    }

    // iterate through all the current task groups and make sure each one has the desired number of replica tasks
    boolean createdTask = false;
    for (Entry<Integer, TaskGroup> entry : activelyReadingTaskGroups.entrySet()) {
      TaskGroup taskGroup = entry.getValue();
      Integer groupId = entry.getKey();

      if (taskGroup.startingSequences == null ||
          taskGroup.startingSequences.size() == 0 ||
          taskGroup.startingSequences.values().stream().allMatch(x -> x == null || isEndOfShard(x))) {
        log.debug("Nothing to read in any partition for taskGroup [%d], skipping task creation", groupId);
        continue;
      }

      if (ioConfig.getReplicas() > taskGroup.tasks.size()) {
        log.info(
            "Number of tasks [%d] does not match configured numReplicas [%d] in task group [%d], creating more tasks",
            taskGroup.tasks.size(), ioConfig.getReplicas(), groupId
        );
        createTasksForGroup(groupId, ioConfig.getReplicas() - taskGroup.tasks.size());
        createdTask = true;
      }
    }

    if (createdTask && firstRunTime.isBeforeNow()) {
      // Schedule a run event after a short delay to update our internal data structures with the new tasks that were
      // just created. This is mainly for the benefit of the status API in situations where the run period is lengthy.
      scheduledExec.schedule(buildRunTask(), 5000, TimeUnit.MILLISECONDS);
    }
  }

  private void addNotice(Notice notice)
  {
    notices.add(notice);
  }

  @VisibleForTesting
  public void moveTaskGroupToPendingCompletion(int taskGroupId)
  {
    final TaskGroup taskGroup = activelyReadingTaskGroups.remove(taskGroupId);
    if (taskGroup != null) {
      pendingCompletionTaskGroups.computeIfAbsent(taskGroupId, k -> new CopyOnWriteArrayList<>()).add(taskGroup);
    }
  }

  @VisibleForTesting
  public int getNoticesQueueSize()
  {
    return notices.size();
  }

  private Map<PartitionIdType, OrderedSequenceNumber<SequenceOffsetType>> generateStartingSequencesForPartitionGroup(
      int groupId
  )
  {
    ImmutableMap.Builder<PartitionIdType, OrderedSequenceNumber<SequenceOffsetType>> builder = ImmutableMap.builder();
    for (PartitionIdType partitionId : partitionGroups.get(groupId)) {
      SequenceOffsetType sequence = partitionOffsets.get(partitionId);

      if (!getNotSetMarker().equals(sequence)) {
        // if we are given a startingOffset (set by a previous task group which is pending completion) then use it
        if (!isEndOfShard(sequence)) {
          builder.put(partitionId, makeSequenceNumber(sequence, useExclusiveStartSequenceNumberForNonFirstSequence()));
        }
      } else {
        // if we don't have a startingOffset (first run or we had some previous failures and reset the sequences) then
        // get the sequence from metadata storage (if available) or Kafka/Kinesis (otherwise)
        OrderedSequenceNumber<SequenceOffsetType> offsetFromStorage = getOffsetFromStorageForPartition(partitionId);

        if (offsetFromStorage != null) {
          builder.put(partitionId, offsetFromStorage);
        }
      }
    }
    return builder.build();
  }

  /**
   * Queries the dataSource metadata table to see if there is a previous ending sequence for this partition. If it
   * doesn't find any data, it will retrieve the latest or earliest Kafka/Kinesis sequence depending on the
   * {@link SeekableStreamSupervisorIOConfig#useEarliestSequenceNumber}.
   */
  private OrderedSequenceNumber<SequenceOffsetType> getOffsetFromStorageForPartition(PartitionIdType partition)
  {
    final Map<PartitionIdType, SequenceOffsetType> metadataOffsets = getOffsetsFromMetadataStorage();
    SequenceOffsetType sequence = metadataOffsets.get(partition);
    if (sequence != null) {
      log.debug("Getting sequence [%s] from metadata storage for partition [%s]", sequence, partition);
      if (!taskTuningConfig.isSkipSequenceNumberAvailabilityCheck()) {
        if (!checkOffsetAvailability(partition, sequence)) {
          if (taskTuningConfig.isResetOffsetAutomatically()) {
            resetInternal(
                createDataSourceMetaDataForReset(ioConfig.getStream(), ImmutableMap.of(partition, sequence))
            );
            throw new StreamException(
                new ISE(
                    "Previous sequenceNumber [%s] is no longer available for partition [%s] - automatically resetting"
                    + " sequence",
                    sequence,
                    partition
                )
            );
          } else {
            throw new StreamException(
                new ISE(
                    "Previous sequenceNumber [%s] is no longer available for partition [%s]. You can clear the previous"
                    + " sequenceNumber and start reading from a valid message by using the supervisor's reset API.",
                    sequence,
                    partition
                )
            );
          }
        }
      }
      return makeSequenceNumber(sequence, useExclusiveStartSequenceNumberForNonFirstSequence());
    } else {
      boolean useEarliestSequenceNumber = ioConfig.isUseEarliestSequenceNumber();
      if (subsequentlyDiscoveredPartitions.contains(partition)) {
        log.info(
            "Overriding useEarliestSequenceNumber and starting from beginning of newly discovered partition [%s] (which is probably from a split or merge)",
            partition
        );
        useEarliestSequenceNumber = true;
      }

      sequence = getOffsetFromStreamForPartition(partition, useEarliestSequenceNumber);
      if (sequence == null) {
        throw new ISE("unable to fetch sequence number for partition[%s] from stream", partition);
      }
      log.debug("Getting sequence number [%s] for partition [%s]", sequence, partition);
      return makeSequenceNumber(sequence, false);
    }
  }

  private Map<PartitionIdType, SequenceOffsetType> getOffsetsFromMetadataStorage()
  {
    final DataSourceMetadata dataSourceMetadata = indexerMetadataStorageCoordinator.retrieveDataSourceMetadata(
        dataSource);
    if (dataSourceMetadata instanceof SeekableStreamDataSourceMetadata
        && checkSourceMetadataMatch(dataSourceMetadata)) {
      @SuppressWarnings("unchecked")
      SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> partitions = ((SeekableStreamDataSourceMetadata) dataSourceMetadata)
          .getSeekableStreamSequenceNumbers();
      if (partitions != null) {
        if (!ioConfig.getStream().equals(partitions.getStream())) {
          log.warn(
              "Topic/stream in metadata storage [%s] doesn't match spec topic/stream [%s], ignoring stored sequences",
              partitions.getStream(),
              ioConfig.getStream()
          );
          return Collections.emptyMap();
        } else if (partitions.getPartitionSequenceNumberMap() != null) {
          return partitions.getPartitionSequenceNumberMap();
        }
      }
    }

    return Collections.emptyMap();
  }

  /**
   * Fetches the earliest or latest offset from the stream via the {@link RecordSupplier}
   */
  @Nullable
  private SequenceOffsetType getOffsetFromStreamForPartition(PartitionIdType partition, boolean useEarliestOffset)
  {
    recordSupplierLock.lock();
    try {
      StreamPartition<PartitionIdType> topicPartition = new StreamPartition<>(ioConfig.getStream(), partition);
      if (!recordSupplier.getAssignment().contains(topicPartition)) {
        // this shouldn't happen, but in case it does...
        throw new IllegalStateException("Record supplier does not match current known partitions");
      }

      return useEarliestOffset
             ? recordSupplier.getEarliestSequenceNumber(topicPartition)
             : recordSupplier.getLatestSequenceNumber(topicPartition);
    }
    finally {
      recordSupplierLock.unlock();
    }
  }

  private void createTasksForGroup(int groupId, int replicas)
      throws JsonProcessingException
  {
    TaskGroup group = activelyReadingTaskGroups.get(groupId);
    Map<PartitionIdType, SequenceOffsetType> startPartitions = group.startingSequences;
    Map<PartitionIdType, SequenceOffsetType> endPartitions = new HashMap<>();
    for (PartitionIdType partition : startPartitions.keySet()) {
      endPartitions.put(partition, getEndOfPartitionMarker());
    }
    Set<PartitionIdType> exclusiveStartSequenceNumberPartitions = activelyReadingTaskGroups
        .get(groupId)
        .exclusiveStartSequenceNumberPartitions;

    DateTime minimumMessageTime = group.minimumMessageTime.orNull();
    DateTime maximumMessageTime = group.maximumMessageTime.orNull();

    SeekableStreamIndexTaskIOConfig newIoConfig = createTaskIoConfig(
        groupId,
        startPartitions,
        endPartitions,
        group.baseSequenceName,
        minimumMessageTime,
        maximumMessageTime,
        exclusiveStartSequenceNumberPartitions,
        ioConfig
    );

    List<SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType>> taskList = createIndexTasks(
        replicas,
        group.baseSequenceName,
        sortingMapper,
        group.checkpointSequences,
        newIoConfig,
        taskTuningConfig,
        rowIngestionMetersFactory
    );

    for (SeekableStreamIndexTask indexTask : taskList) {
      Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
      if (taskQueue.isPresent()) {
        try {
          taskQueue.get().add(indexTask);
        }
        catch (EntryExistsException e) {
          stateManager.recordThrowableEvent(e);
          log.error("Tried to add task [%s] but it already exists", indexTask.getId());
        }
      } else {
        log.error("Failed to get task queue because I'm not the leader!");
      }
    }
  }

  /**
   * monitoring method, fetches current partition offsets and lag in a background reporting thread
   */
  @VisibleForTesting
  public void updateCurrentAndLatestOffsets()
  {
    // if we aren't in a steady state, chill out for a bit, don't worry, we'll get called later, but if we aren't
    // healthy go ahead and try anyway to try if possible to provide insight into how much time is left to fix the
    // issue for cluster operators since this feeds the lag metrics
    if (stateManager.isSteadyState() || !stateManager.isHealthy()) {
      try {
        updateCurrentOffsets();
        updatePartitionLagFromStream();
        sequenceLastUpdated = DateTimes.nowUtc();
      }
      catch (Exception e) {
        log.warn(e, "Exception while getting current/latest sequences");
      }
    }
  }

  private void updateCurrentOffsets() throws InterruptedException, ExecutionException, TimeoutException
  {
    final List<ListenableFuture<Void>> futures = Stream.concat(
        activelyReadingTaskGroups.values().stream().flatMap(taskGroup -> taskGroup.tasks.entrySet().stream()),
        pendingCompletionTaskGroups.values()
                                   .stream()
                                   .flatMap(List::stream)
                                   .flatMap(taskGroup -> taskGroup.tasks.entrySet().stream())
    ).map(
        task -> Futures.transform(
            taskClient.getCurrentOffsetsAsync(task.getKey(), false),
            (Function<Map<PartitionIdType, SequenceOffsetType>, Void>) (currentSequences) -> {

              if (currentSequences != null && !currentSequences.isEmpty()) {
                task.getValue().currentSequences = currentSequences;
              }

              return null;
            }
        )
    ).collect(Collectors.toList());

    Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
  }

  protected abstract void updatePartitionLagFromStream();

  /**
   * Gets 'lag' of currently processed offset behind latest offset as a measure of difference between offsets.
   */
  @Nullable
  protected abstract Map<PartitionIdType, Long> getPartitionRecordLag();

  /**
   * Gets 'lag' of currently processed offset behind latest offset as a measure of the difference in time inserted.
   */
  @Nullable
  protected abstract Map<PartitionIdType, Long> getPartitionTimeLag();

  protected Map<PartitionIdType, SequenceOffsetType> getHighestCurrentOffsets()
  {
    if (!spec.isSuspended()) {
      if (activelyReadingTaskGroups.size() > 0 || pendingCompletionTaskGroups.size() > 0) {
        return activelyReadingTaskGroups
            .values()
            .stream()
            .flatMap(taskGroup -> taskGroup.tasks.entrySet().stream())
            .flatMap(taskData -> taskData.getValue().currentSequences.entrySet().stream())
            .collect(Collectors.toMap(
                Entry::getKey,
                Entry::getValue,
                (v1, v2) -> makeSequenceNumber(v1).compareTo(makeSequenceNumber(v2)) > 0 ? v1 : v2
            ));
      }
      // nothing is running but we are not suspended, so lets just hang out in case we get called while things start up
      return ImmutableMap.of();
    } else {
      // if supervisor is suspended, no tasks are likely running so use offsets in metadata, if exist
      return getOffsetsFromMetadataStorage();
    }
  }

  private OrderedSequenceNumber<SequenceOffsetType> makeSequenceNumber(SequenceOffsetType seq)
  {
    return makeSequenceNumber(seq, false);
  }

  // exposed for testing for visibility into initialization state
  @VisibleForTesting
  public boolean isStarted()
  {
    return started;
  }

  // exposed for testing for visibility into initialization state
  @VisibleForTesting
  public boolean isLifecycleStarted()
  {
    return lifecycleStarted;
  }

  // exposed for testing for visibility into initialization state
  @VisibleForTesting
  public int getInitRetryCounter()
  {
    return initRetryCounter;
  }

  // exposed for testing to allow "bootstrap.servers" to be changed after supervisor is created
  @VisibleForTesting
  public SeekableStreamSupervisorIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @Override
  public void checkpoint(int taskGroupId, DataSourceMetadata checkpointMetadata)
  {
    Preconditions.checkNotNull(checkpointMetadata, "checkpointMetadata");

    //noinspection unchecked
    final SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> seekableMetadata =
        (SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType>) checkpointMetadata;

    Preconditions.checkArgument(
        spec.getIoConfig().getStream().equals(seekableMetadata.getSeekableStreamSequenceNumbers().getStream()),
        "Supervisor stream [%s] and stream in checkpoint [%s] does not match",
        spec.getIoConfig().getStream(),
        seekableMetadata.getSeekableStreamSequenceNumbers().getStream()
    );
    Preconditions.checkArgument(
        seekableMetadata.getSeekableStreamSequenceNumbers() instanceof SeekableStreamStartSequenceNumbers,
        "checkpointMetadata must be SeekableStreamStartSequenceNumbers"
    );

    log.info("Checkpointing [%s] for taskGroup [%s]", checkpointMetadata, taskGroupId);
    addNotice(new CheckpointNotice(taskGroupId, seekableMetadata));
  }

  @VisibleForTesting
  public Map<String, Object> createBaseTaskContexts()
  {
    final Map<String, Object> contexts = new HashMap<>();
    if (spec.getContext() != null) {
      contexts.putAll(spec.getContext());
    }
    return contexts;
  }

  @VisibleForTesting
  public ConcurrentHashMap<Integer, Set<PartitionIdType>> getPartitionGroups()
  {
    return partitionGroups;
  }

  @VisibleForTesting
  public boolean isPartitionIdsEmpty()
  {
    return this.partitionIds.isEmpty();
  }

  public ConcurrentHashMap<PartitionIdType, SequenceOffsetType> getPartitionOffsets()
  {
    return partitionOffsets;
  }

  /**
   * Should never be called outside of tests.
   */
  @VisibleForTesting
  public void setPartitionIdsForTests(
      List<PartitionIdType> partitionIdsForTests
  )
  {
    partitionIds.clear();
    partitionIds.addAll(partitionIdsForTests);
  }

  /**
   * creates a specific task IOConfig instance for Kafka/Kinesis
   *
   * @return specific instance of Kafka/Kinesis IOConfig
   */
  protected abstract SeekableStreamIndexTaskIOConfig createTaskIoConfig(
      int groupId,
      Map<PartitionIdType, SequenceOffsetType> startPartitions,
      Map<PartitionIdType, SequenceOffsetType> endPartitions,
      String baseSequenceName,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      Set<PartitionIdType> exclusiveStartSequenceNumberPartitions,
      SeekableStreamSupervisorIOConfig ioConfig
  );

  /**
   * creates a list of specific kafka/kinesis index tasks using
   * the given replicas count
   *
   * @return list of specific kafka/kinesis index taksks
   *
   * @throws JsonProcessingException
   */
  protected abstract List<SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType>> createIndexTasks(
      int replicas,
      String baseSequenceName,
      ObjectMapper sortingMapper,
      TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>> sequenceOffsets,
      SeekableStreamIndexTaskIOConfig taskIoConfig,
      SeekableStreamIndexTaskTuningConfig taskTuningConfig,
      RowIngestionMetersFactory rowIngestionMetersFactory
  ) throws JsonProcessingException;

  /**
   * calculates the taskgroup id that the given partition belongs to.
   * different between Kafka/Kinesis since Kinesis uses String as partition id
   *
   * @param partition partition id
   *
   * @return taskgroup id
   */
  protected abstract int getTaskGroupIdForPartition(PartitionIdType partition);

  /**
   * checks if the passed in DataSourceMetadata is a specific instance
   * of [kafka/kinesis]DataSourceMetadata
   *
   * @param metadata datasource metadata
   *
   * @return true if isInstance else false
   */
  protected abstract boolean checkSourceMetadataMatch(DataSourceMetadata metadata);

  /**
   * checks if the passed in Task is a specific instance of
   * [Kafka/Kinesis]IndexTask
   *
   * @param task task
   *
   * @return true if isInstance else false
   */
  protected abstract boolean doesTaskTypeMatchSupervisor(Task task);

  /**
   * creates a specific instance of kafka/kinesis datasource metadata. Only used for reset.
   *
   * @param stream stream name
   * @param map    partitionId -> sequence
   *
   * @return specific instance of datasource metadata
   */
  protected abstract SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> createDataSourceMetaDataForReset(
      String stream,
      Map<PartitionIdType, SequenceOffsetType> map
  );

  /**
   * wraps the passed in SequenceOffsetType sequence number into a {@link OrderedSequenceNumber} object
   * to facilitate comparison and accomodate exclusive starting sequennce in kinesis
   *
   * @return specific instance of [Kafka/Kinesis]OrderedSequenceNumber
   */
  protected abstract OrderedSequenceNumber<SequenceOffsetType> makeSequenceNumber(
      SequenceOffsetType seq,
      boolean isExclusive
  );

  /**
   * default implementation, schedules periodic fetch of latest offsets and {@link #emitLag} reporting for Kafka and Kinesis
   */
  protected void scheduleReporting(ScheduledExecutorService reportingExec)
  {
    SeekableStreamSupervisorIOConfig ioConfig = spec.getIoConfig();
    SeekableStreamSupervisorTuningConfig tuningConfig = spec.getTuningConfig();
    reportingExec.scheduleAtFixedRate(
        this::updateCurrentAndLatestOffsets,
        ioConfig.getStartDelay().getMillis() + INITIAL_GET_OFFSET_DELAY_MILLIS, // wait for tasks to start up
        Math.max(
            tuningConfig.getOffsetFetchPeriod().getMillis(), MINIMUM_GET_OFFSET_PERIOD_MILLIS
        ),
        TimeUnit.MILLISECONDS
    );

    reportingExec.scheduleAtFixedRate(
        this::emitLag,
        ioConfig.getStartDelay().getMillis() + INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS, // wait for tasks to start up
        spec.getMonitorSchedulerConfig().getEmitterPeriod().getMillis(),
        TimeUnit.MILLISECONDS
    );
  }

  /**
   * calculate lag per partition for kafka as a measure of message count, kinesis implementation returns an empty
   * map
   *
   * @return map of partition id -> lag
   */
  protected abstract Map<PartitionIdType, Long> getRecordLagPerPartition(
      Map<PartitionIdType, SequenceOffsetType> currentOffsets
  );

  protected abstract Map<PartitionIdType, Long> getTimeLagPerPartition(
      Map<PartitionIdType, SequenceOffsetType> currentOffsets
  );

  /**
   * returns an instance of a specific Kinesis/Kafka recordSupplier
   *
   * @return specific instance of Kafka/Kinesis RecordSupplier
   */
  protected abstract RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> setupRecordSupplier();

  /**
   * creates a specific instance of Kafka/Kinesis Supervisor Report Payload
   *
   * @return specific instance of Kafka/Kinesis Supervisor Report Payload
   */
  protected abstract SeekableStreamSupervisorReportPayload<PartitionIdType, SequenceOffsetType> createReportPayload(
      int numPartitions,
      boolean includeOffsets
  );

  /**
   * checks if offset from metadata storage is still valid
   *
   * @return true if still valid else false
   */
  private boolean checkOffsetAvailability(
      @NotNull PartitionIdType partition,
      @NotNull SequenceOffsetType offsetFromMetadata
  )
  {
    final SequenceOffsetType earliestOffset = getOffsetFromStreamForPartition(partition, true);
    return earliestOffset != null
           && makeSequenceNumber(earliestOffset).compareTo(makeSequenceNumber(offsetFromMetadata)) <= 0;
  }

  protected void emitLag()
  {
    if (spec.isSuspended() || !stateManager.isSteadyState()) {
      // don't emit metrics if supervisor is suspended or not in a healthy running state
      // (lag should still available in status report)
      return;
    }
    try {
      Map<PartitionIdType, Long> partitionRecordLags = getPartitionRecordLag();
      Map<PartitionIdType, Long> partitionTimeLags = getPartitionTimeLag();

      if (partitionRecordLags == null && partitionTimeLags == null) {
        throw new ISE("Latest offsets have not been fetched");
      }
      final String type = spec.getType();

      BiConsumer<Map<PartitionIdType, Long>, String> emitFn = (partitionLags, suffix) -> {
        if (partitionLags == null) {
          return;
        }
        ArrayList<Long> lags = new ArrayList<>(3);
        computeLags(partitionLags, lags);
        emitter.emit(
            ServiceMetricEvent.builder()
                              .setDimension("dataSource", dataSource)
                              .build(StringUtils.format("ingest/%s/lag%s", type, suffix), lags.get(1))
        );
        emitter.emit(
            ServiceMetricEvent.builder()
                              .setDimension("dataSource", dataSource)
                              .build(StringUtils.format("ingest/%s/maxLag%s", type, suffix), lags.get(0))
        );
        emitter.emit(
            ServiceMetricEvent.builder()
                              .setDimension("dataSource", dataSource)
                              .build(StringUtils.format("ingest/%s/avgLag%s", type, suffix), lags.get(2))
        );
      };

      // this should probably really be /count or /records or something.. but keeping like this for backwards compat
      emitFn.accept(partitionRecordLags, "");
      emitFn.accept(partitionTimeLags, "/time");
    }
    catch (Exception e) {
      log.warn(e, "Unable to compute lag");
    }
  }


  /**
   * This method compute maxLag, totalLag and avgLag then fill in 'lags'
   * @param partitionLags lags per partition
   * @param lags a arraylist in order of maxLag, totalLag and avgLag
   */
  protected void computeLags(Map<PartitionIdType, Long> partitionLags, ArrayList<Long> lags)
  {
    long maxLag = 0, totalLag = 0, avgLag;
    for (long lag : partitionLags.values()) {
      if (lag > maxLag) {
        maxLag = lag;
      }
      totalLag += lag;
    }
    avgLag = partitionLags.size() == 0 ? 0 : totalLag / partitionLags.size();
    lags.add(maxLag);
    lags.add(totalLag);
    lags.add(avgLag);
  }

  /**
   * a special sequence number that is used to indicate that the sequence offset
   * for a particular partition has not yet been calculated by the supervisor. When
   * the not_set marker is read by the supervisor, it will first attempt to restore it
   * from metadata storage, if that fails, from the Kafka/Kinesis
   *
   * @return sequence offset that represets NOT_SET
   */
  protected abstract SequenceOffsetType getNotSetMarker();

  /**
   * returns the logical maximum number for a Kafka partition or Kinesis shard. This is
   * used to set the initial endoffsets when creating a new task, since we don't know
   * what sequence offsets to read to initially
   *
   * @return end of partition sequence offset
   */
  protected abstract SequenceOffsetType getEndOfPartitionMarker();

  /**
   * checks if seqNum marks the end of a Kinesis shard. This indicates that the shard is closed. Used by Kinesis only.
   */
  protected abstract boolean isEndOfShard(SequenceOffsetType seqNum);

  /**
   * checks if seqNum marks an expired Kinesis shard. Used by Kinesis only.
   */
  protected abstract boolean isShardExpirationMarker(SequenceOffsetType seqNum);

  /**
   * Returns true if the start sequence number should be exclusive for the non-first sequences for the whole partition.
   * For example, in Kinesis, the start offsets are inclusive for the first sequence, but exclusive for following
   * sequences. In Kafka, start offsets are always inclusive.
   */
  protected abstract boolean useExclusiveStartSequenceNumberForNonFirstSequence();

  /**
   * Collect maxLag, totalLag, avgLag into ArrayList<Long> lags
   * Only support Kafka ingestion so far.
   * @param lags , Notice : The order of values is maxLag, totalLag and avgLag.
   */
  protected abstract void collectLag(ArrayList<Long> lags);
}
