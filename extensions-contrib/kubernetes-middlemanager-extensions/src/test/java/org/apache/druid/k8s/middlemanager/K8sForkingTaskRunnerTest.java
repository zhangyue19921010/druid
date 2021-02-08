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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.kubernetes.client.PodLogs;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarBuilder;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1OwnerReferenceBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodBuilder;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.k8s.middlemanager.common.DefaultK8sApiClient;
import org.apache.druid.k8s.middlemanager.common.K8sApiClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;

public class K8sForkingTaskRunnerTest extends EasyMockSupport
{
  private ApiClient realK8sClient;
  private CoreV1Api coreV1Api;
  private GenericKubernetesApi<V1Pod, V1PodList> podClient;
  private K8sApiClient k8sApiClient;
  private TaskLogPusher taskLogPusher;
  private ObjectMapper jsonMapper;
  private PodLogs podLogs;
  private static final EmittingLogger LOGGER = new EmittingLogger(K8sForkingTaskRunnerTest.class);


  @Before
  public void setUp()
  {
    this.realK8sClient = EasyMock.mock(ApiClient.class);
    this.coreV1Api = EasyMock.mock(CoreV1Api.class);
    this.podClient = EasyMock.mock(GenericKubernetesApi.class);
    this.podLogs = EasyMock.mock(PodLogs.class);
    DefaultK8sApiClient defaultK8sApiClient = new TesableDefaultK8sApiClient(realK8sClient, null);
    defaultK8sApiClient.setCoreV1Api(coreV1Api);
    defaultK8sApiClient.setPodClient(podClient);
    defaultK8sApiClient.setPodLogsClient(podLogs);
    this.k8sApiClient = defaultK8sApiClient;
    this.taskLogPusher = EasyMock.mock(TaskLogPusher.class);
    this.jsonMapper = new DefaultObjectMapper();
  }

  @Test(timeout = 60_000L)
  public void testK8sForkingTaskRunnerRunAndStop() throws Exception
  {
    DruidNode node = new DruidNode("forkServiceName", "0.0.0.0", false, 8000, null, true, false);
    ForkingTaskRunnerConfig forkingTaskRunnerConfig = jsonMapper.convertValue(ImmutableMap.of(
            "javaOpts", "ab",
            "classpath", "/aaa"), ForkingTaskRunnerConfig.class);
    TaskConfig taskConfig = new TaskConfig(null, null, null, null, null, false, null, null, null);
    WorkerConfig workerConfig = new WorkerConfig();
    Properties properties = new Properties();
    StartupLoggingConfig startupLoggingConfig = new StartupLoggingConfig();

    K8sForkingTaskRunner k8sForkingTaskRunner = new K8sForkingTaskRunner(
            forkingTaskRunnerConfig,
            taskConfig,
            workerConfig,
            properties,
            taskLogPusher,
            jsonMapper,
            node,
            startupLoggingConfig,
            k8sApiClient);

    Task task = new NoopTask("forktaskid", null, null, 0, 0, null, null, null);
    String taskID = task.getId();
    final File taskDir = taskConfig.getTaskDir(task.getId());
    EasyMock.expect(coreV1Api.listNamespacedConfigMap(EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull()))
            .andReturn(null)
            .anyTimes();

    V1OwnerReference owner = new V1OwnerReferenceBuilder()
            .withName(System.getenv("POD_NAME"))
            .withApiVersion("v1")
            .withUid(System.getenv("POD_UID"))
            .withKind("Pod")
            .withController(true)
            .build();

    V1ConfigMap configMap = new V1ConfigMapBuilder()
            .withNewMetadata()
            .withOwnerReferences(owner)
            .withName(taskID)
            .withLabels(ImmutableMap.of("druid.ingest.task.id", taskID))
            .endMetadata()
            .withData(ImmutableMap.of("task.json", jsonMapper.writeValueAsString(task)))
            .build();

    EasyMock.expect(coreV1Api.createNamespacedConfigMap("default", configMap, null, null, null))
            .andReturn(configMap)
            .anyTimes();

    final String configMapVolumeName = "task-json-vol-tmp";

    V1VolumeMount configMapVolumeMount = new V1VolumeMount().name(configMapVolumeName).mountPath("/druidTmp");
    V1ConfigMapVolumeSource configMapVolume = new V1ConfigMapVolumeSource().defaultMode(420).name(taskID);

    ArrayList<V1VolumeMount> v1VolumeMounts = new ArrayList<>();
    v1VolumeMounts.add(configMapVolumeMount);

    V1EnvVar podIpEnv = new V1EnvVarBuilder()
            .withName("POD_IP")
            .withNewValueFrom()
            .withFieldRef(new V1ObjectFieldSelector().fieldPath("status.podIP"))
            .endValueFrom()
            .build();

    V1EnvVar task_id = new V1EnvVarBuilder()
            .withName("TASK_ID")
            .withNewValue(taskID)
            .build();

    V1EnvVar task_dir = new V1EnvVarBuilder()
            .withName("TASK_DIR")
            .withNewValue(taskDir.getAbsolutePath()).build();

    V1EnvVar task_json_tmp_location = new V1EnvVarBuilder()
            .withName("TASK_JSON_TMP_LOCATION")
            .withNewValue("/druidTmp" + "/task.json").build();

    V1Pod pod = new V1PodBuilder()
            .withNewMetadata()
            .withOwnerReferences(owner)
            .withNamespace("default")
            .withName(taskID)
            .withLabels(ImmutableMap.of("druid.ingest.task.id", taskID))
            .endMetadata()
            .withNewSpec()
            .withNewSecurityContext()
            .withFsGroup(0L)
            .withRunAsGroup(0L)
            .withRunAsUser(0L)
            .endSecurityContext()
            .addNewVolume()
            .withNewName(configMapVolumeName)
            .withConfigMap(configMapVolume)
            .endVolume()
            .withNewRestartPolicy("Never")
            .addNewContainer()
            .withPorts(new V1ContainerPort().protocol("TCP").containerPort(8100).name("http"))
            .withNewSecurityContext()
            .withNewPrivileged(true)
            .endSecurityContext()
            .withCommand("/bin/sh", "-c")
            .withArgs("")
            .withName("peon")
            .withImage("druid/cluster:v1")
            .withImagePullPolicy("IfNotPresent")
            .withVolumeMounts(v1VolumeMounts)
            .withEnv(ImmutableList.of(podIpEnv, task_id, task_dir, task_json_tmp_location))
            .withNewResources()
            .withRequests(ImmutableMap.of("cpu", Quantity.fromString("1"), "memory", Quantity.fromString("2G")))
            .withLimits(ImmutableMap.of("cpu", Quantity.fromString("1"), "memory", Quantity.fromString("2G")))
            .endResources()
            .endContainer()
            .endSpec()
            .withNewStatus()
            .addNewPodIP()
            .withNewIp("0.0.0.1")
            .endPodIP()
            .endStatus()
            .build();

    EasyMock.expect(coreV1Api.createNamespacedPod(EasyMock.anyString(), EasyMock.anyObject(V1Pod.class), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull()))
            .andReturn(pod)
            .anyTimes();

    V1PodList v1PodListEmpty = new V1PodList();
    V1PodList v1PodList = new V1PodList().addItemsItem(pod);
    EasyMock.expect(coreV1Api.listNamespacedPod(EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull()))
            .andReturn(v1PodListEmpty).once();
    EasyMock.expect(coreV1Api.listNamespacedPod(EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull()))
            .andReturn(v1PodList).once();
    EasyMock.expect(coreV1Api.deleteCollectionNamespacedConfigMap(EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.anyInt(), EasyMock.anyString(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull())).andReturn(null).anyTimes();
    EasyMock.replay(coreV1Api);


    KubernetesApiResponse<V1Pod> response = new KubernetesApiResponse<V1Pod>(pod);
    EasyMock.expect(podClient.get(EasyMock.anyString(), EasyMock.anyString())).andReturn(response).anyTimes();
    EasyMock.expect(podClient.delete(EasyMock.anyString(), EasyMock.anyString())).andReturn(null).anyTimes();
    EasyMock.replay(podClient);

    EasyMock.expect(podLogs.streamNamespacedPodLog(EasyMock.anyObject(V1Pod.class))).andReturn(new FileInputStream(new File("src/test/resources/logExample.txt"))).anyTimes();
    EasyMock.replay(podLogs);

    ListenableFuture<TaskStatus> taskFuture = k8sForkingTaskRunner.run(task);
    while (!taskFuture.isDone()) {
      Thread.sleep(1000);
    }
    TaskStatus taskStatus = taskFuture.get();
    Assert.assertTrue(taskStatus.getStatusCode().isSuccess());
  }


  private static class TesableDefaultK8sApiClient extends DefaultK8sApiClient
  {
    public TesableDefaultK8sApiClient(ApiClient realK8sClient, ObjectMapper jsonMapper)
    {
      super(realK8sClient, jsonMapper);
    }

    @Override
    public String getPodStatus(V1Pod peonPod)
    {
      return "Succeeded";
    }
  }
}
