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

package org.apache.druid.k8s.middlemanager.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.kubernetes.client.PodLogs;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Concrete {@link K8sApiClient} impl using k8s-client java lib.
 */
public class DefaultK8sApiClient implements K8sApiClient
{
  private static final Logger LOGGER = new Logger(DefaultK8sApiClient.class);

  private final ApiClient realK8sClient;
  private final CoreV1Api coreV1Api;
  private final ObjectMapper jsonMapper;
  private GenericKubernetesApi<V1Pod, V1PodList> podClient;

  @Inject
  public DefaultK8sApiClient(ApiClient realK8sClient, @Json ObjectMapper jsonMapper)
  {
    this.realK8sClient = realK8sClient;
    this.coreV1Api = new CoreV1Api(realK8sClient);
    this.jsonMapper = jsonMapper;
    this.podClient = new GenericKubernetesApi<>(V1Pod.class, V1PodList.class, "", "v1", "pods", realK8sClient);
  }

  @Override
  public void patchPod(String podName, String podNamespace, String jsonPatchStr)
  {
    try {
      coreV1Api.patchNamespacedPod(podName, podNamespace, new V1Patch(jsonPatchStr), "true", null, null, null);
    }
    catch (ApiException ex) {
      throw new RE(ex, "Failed to patch pod[%s/%s], code[%d], error[%s].", podNamespace, podName, ex.getCode(), ex.getResponseBody());
    }
  }

  /**
   * need to be done:
   * 1. create dir attemptDir and taskDir
   * 2. port
   * 3.
   * @return
   */
  @Override
  public V1Pod createPod(String taskID, String image, String namespace, Map<String, String> labels, Map<String, Quantity> resourceLimit, File taskDir, List<String> args, int childPort, int tlsChildPort)
  {
    // remove 'java' in args
    args.remove(0);

    V1EnvVar podIpEnv = new V1EnvVarBuilder()
            .withName("POD_IP")
            .withNewValueFrom()
            .withFieldRef(new V1ObjectFieldSelector().fieldPath("status.podIP"))
            .endValueFrom()
            .build();

    V1Pod pod = new V1PodBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .withName(taskID + "-pod")
            .withLabels(labels)
            .endMetadata()
            .withNewSpec()
            .addNewVolume()
            .withNewName("task-json-vol")
            .withConfigMap(new V1ConfigMapVolumeSource().defaultMode(420).name(taskID))
            .endVolume()
            .addNewContainer()
            .withPorts(new V1ContainerPort().protocol("TCP").containerPort(childPort).name("http"))
            .withPorts(new V1ContainerPort().protocol("TCP").containerPort(tlsChildPort).name("https"))
            .addNewCommand("java")
            .withArgs(args)
            .withName("peon")
            .withImage(image)
            .withVolumeMounts(new V1VolumeMount().name("task-json-vol").mountPath(taskDir.getAbsolutePath()))
            .withEnv(podIpEnv)
            .withNewResources()
            .withLimits(resourceLimit)
            .endResources()
            .endContainer()
            .endSpec()
            .build();
    try {
      return podClient.create(pod).throwsApiException().getObject();
    }
    catch (ApiException e) {
      LOGGER.warn(e, "Error when create peon pod.");
    }
    return null;
  }

  @Override
  public V1ConfigMap createConfigMap(String namespace, String configmapName, Map<String, String> labels, Map<String, String> data)
  {
    V1ConfigMap configMap = new V1ConfigMapBuilder()
            .withNewMetadata()
            .withName(configmapName)
            .withLabels(labels)
            .endMetadata()
            .withData(data)
            .build();

    try {
      V1ConfigMap conf = coreV1Api.createNamespacedConfigMap(namespace, configMap, null, null, null);
      return conf;
    }
    catch (ApiException e) {
      LOGGER.warn(e, "Error when create configMap.");
    }
    return null;
  }

  @Override
  public Boolean configMapIsExist(String namespace, String labelSelector)
  {

    try {
      V1ConfigMapList v1ConfigMapList = coreV1Api.listNamespacedConfigMap(namespace, null, null, null, null, labelSelector, null, null, null, null);
      return !v1ConfigMapList.getItems().isEmpty();
    }
    catch (ApiException e) {
      LOGGER.warn(e, "Error when get configMaps.");
    }
    return false;
  }

  @Override
  public void waitForPodCreate(V1Pod peonPod, String labelSelector)
  {
    try {
      V1PodList v1PodList = coreV1Api.listNamespacedPod(peonPod.getMetadata().getNamespace(), null, null, null, null, labelSelector, null, null, null, null);
      while (v1PodList.getItems().isEmpty()) {
        Thread.sleep(3 * 1000);
        v1PodList = coreV1Api.listNamespacedPod(peonPod.getMetadata().getNamespace(), null, null, null, null, labelSelector, null, null, null, null);
      }
    }
    catch (Exception e) {
      LOGGER.warn(e, "Error when wait for pod creating.");
    }
  }

  @Override
  public InputStream getPodLogs(V1Pod peonPod)
  {
    PodLogs logs = new PodLogs(realK8sClient);
    InputStream is = null;
    try {
      is = logs.streamNamespacedPodLog(peonPod);
    }
    catch (Exception e) {
      LOGGER.warn(e, "Error when get pod logs.");
    }
    return is;
  }

  @Override
  public String waitForPodFinished(V1Pod peonPod)
  {
    String phase = getPodStatus(peonPod);
    while (true) {
      switch (phase) {
        case "Succeeded": return phase;
        case "Failed": return phase;
        default: phase = getPodStatus(peonPod);
      }
    }
  }
  @Override
  public String getPodStatus(V1Pod peonPod)
  {
    V1ObjectMeta mt = peonPod.getMetadata();
    return podClient.get(mt.getNamespace(), mt.getName()).getObject().getStatus().getPhase();
  }

  @Override
  public void deletePod(V1Pod peonPod)
  {
    V1ObjectMeta mt = peonPod.getMetadata();
    podClient.delete(mt.getNamespace(), mt.getName()).getObject();
  }
}
