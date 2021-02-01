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

package org.apache.druid.k8s.middlemanager;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.k8s.middlemanager.common.DefaultK8sApiClient;
import org.apache.druid.k8s.middlemanager.common.K8sApiClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class K8sForkingTaskRunnerTest extends EasyMockSupport
{
  private ApiClient realK8sClient;
  private CoreV1Api coreV1Api;
  private GenericKubernetesApi<V1Pod, V1PodList> podClient;
  private K8sApiClient k8sApiClient;
  private TaskLogPusher taskLogPusher;
  private ObjectMapper jsonMapper;
  private DruidNode node;


  @Before
  public void setUp()
  {
    this.realK8sClient = EasyMock.mock(ApiClient.class);
    this.coreV1Api = EasyMock.mock(CoreV1Api.class);
    this.podClient = EasyMock.mock(GenericKubernetesApi.class);
    DefaultK8sApiClient defaultK8sApiClient = new DefaultK8sApiClient(realK8sClient, null);
    defaultK8sApiClient.setCoreV1Api(coreV1Api);
    defaultK8sApiClient.setPodClient(podClient);
    this.k8sApiClient = defaultK8sApiClient;
    this.taskLogPusher = EasyMock.mock(TaskLogPusher.class);
    this.jsonMapper = EasyMock.mock(ObjectMapper.class);
    this.node = EasyMock.mock(DruidNode.class);
  }

  @Test
  public void testK8sForkingTAskRunnerRunAndStop()
  {
    ForkingTaskRunnerConfig forkingTaskRunnerConfig = new ForkingTaskRunnerConfig();
    TaskConfig taskConfig = new TaskConfig();
    WorkerConfig workerConfig = new WorkerConfig();
    Properties properties = new Properties();
    StartupLoggingConfig startupLoggingConfig = new StartupLoggingConfig();

    K8sForkingTaskRunner k8sForkingTaskRunner = new K8sForkingTaskRunner(forkingTaskRunnerConfig, taskConfig, workerConfig, properties, taskLogPusher, jsonMapper, node, startupLoggingConfig, k8sApiClient);
    Task task = NoopTask.create();
    k8sForkingTaskRunner.run(task);

  }








}
