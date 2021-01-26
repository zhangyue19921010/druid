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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.kubernetes.client.Copy;
import io.kubernetes.client.PodLogs;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1ConfigMapVolumeSource;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarBuilder;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodBuilder;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.util.Yaml;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
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
  private final GenericKubernetesApi<V1Pod, V1PodList> podClient;

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
  public V1Pod createPod(String taskID, String image, String namespace, Map<String, String> labels, Map<String, Quantity> resourceLimit, File taskDir, List<String> args, int childPort, int tlsChildPort, String tempLoc, String peonPodRestartPolicy)
  {
    try {
      final String volumnName = "task-json-vol-tmp";
      String comands = buildCommands(args);

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
              .withNewValue(tempLoc + "/task.json").build();

      V1Pod pod = new V1PodBuilder()
              .withNewMetadata()
              .withNamespace(namespace)
              .withName(taskID)
              .withLabels(labels)
              .endMetadata()
              .withNewSpec()
              .withNewSecurityContext()
              .withFsGroup(0L)
              .withRunAsGroup(0L)
              .withRunAsUser(0L)
              .endSecurityContext()
              .addNewVolume()
              .withNewName(volumnName)
              .withConfigMap(new V1ConfigMapVolumeSource().defaultMode(420).name(taskID))
              .endVolume()
              .withNewRestartPolicy(peonPodRestartPolicy)
              .addNewContainer()
              .withPorts(new V1ContainerPort().protocol("TCP").containerPort(childPort).name("http"))
              .withNewSecurityContext()
              .withNewPrivileged(true)
              .endSecurityContext()
              .withCommand("/bin/sh", "-c")
              .withArgs(comands)
              .withName("peon")
              .withImage(image)
              .withImagePullPolicy("IfNotPresent")
              .withVolumeMounts(new V1VolumeMount().name(volumnName).mountPath(tempLoc))
              .withEnv(ImmutableList.of(podIpEnv, task_id, task_dir, task_json_tmp_location))
              .withNewResources()
              .withLimits(resourceLimit)
              .endResources()
              .endContainer()
              .endSpec()
              .build();
      V1Pod peonPod = coreV1Api.createNamespacedPod(namespace, pod, null, null, null);
      return peonPod;
    }
    catch (ApiException ex) {
      LOGGER.warn(ex, "Failed to create pod[%s/%s], code[%d], error[%s].", namespace, taskID, ex.getCode(), ex.getResponseBody());
    }
    return null;
  }

  private String buildCommands(List<String> args)
  {
    for (int i = 0; i < args.size(); i++) {
      String value = args.get(i);
      args.set(i, StringUtils.replace(value, "\n", ""));
      args.set(i, StringUtils.replace(value, "\r", ""));
      args.set(i, StringUtils.replace(value, "\t", ""));
      args.set(i, StringUtils.replace(value, "\"", "\\\""));
      String[] splits = args.get(i).split("=");
      if (splits.length > 1 && splits[1].contains(";")) {
        args.set(i, splits[0] + "=" + "\"" + splits[1] + "\"");
      }

      if (splits.length > 1 && (splits[1].startsWith("[") || splits[1].startsWith("{"))) {
        args.set(i, splits[0] + "=" + "\"" + splits[1] + "\"");
      }
    }

    StringBuilder builder = new StringBuilder();
    for (String arg : args) {
      builder.append(arg).append(" ");
    }

    String javaCommands = builder.toString().substring(0, builder.toString().length() - 1);

    final String prepareTaskFiles = "mkdir -p /tmp/conf/;test -d /tmp/conf/druid && rm -r /tmp/conf/druid;cp -r /opt/druid/conf/druid /tmp/conf/druid;mkdir -p $TASK_DIR; cp $TASK_JSON_TMP_LOCATION $TASK_DIR;";
    return prepareTaskFiles + javaCommands;
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
    catch (ApiException ex) {
      LOGGER.warn(ex, "Failed to create configmap[%s/%s], code[%d], error[%s].", namespace, configmapName, ex.getCode(), ex.getResponseBody());
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
    catch (ApiException ex) {
      LOGGER.warn(ex, "Failed to get configmap[%s/%s], code[%d], error[%s].", namespace, labelSelector, ex.getCode(), ex.getResponseBody());
    }
    return false;
  }

  /**
   * There are five status for pod, including "Pending", "Running", "Succeeded", "Failed", "Unknown"
   * Just care about Pending status here.
   * @param peonPod
   * @param labelSelector
   */
  @Override
  public void waitForPodRunning(V1Pod peonPod, String labelSelector)
  {
    String namespace = peonPod.getMetadata().getNamespace();
    String podName = peonPod.getMetadata().getName();
    try {
      V1PodList v1PodList = coreV1Api.listNamespacedPod(namespace, null, null, null, null, labelSelector, null, null, null, null);
      while (v1PodList.getItems().isEmpty() || getPodStatus(v1PodList.getItems().get(0)).equalsIgnoreCase("Pending")) {
        LOGGER.info("Still waitting for pod Running [%s/%s]", namespace, podName);
        Thread.sleep(3 * 1000);
        v1PodList = coreV1Api.listNamespacedPod(peonPod.getMetadata().getNamespace(), null, null, null, null, labelSelector, null, null, null, null);
      }
      LOGGER.info("Peon Pod Running : %s", Yaml.dump(peonPod));
    }
    catch (ApiException ex) {
      LOGGER.warn(ex, "Exception to wait for pod Running[%s/%s], code[%d], error[%s].", namespace, labelSelector, ex.getCode(), ex.getResponseBody());
    }
    catch (InterruptedException ex) {
      LOGGER.warn(ex, "InterruptedException when wait for pod Running [%s/%s]", namespace, podName);
    }
  }

  @Override
  public InputStream getPodLogs(V1Pod peonPod)
  {
    String namespace = peonPod.getMetadata().getNamespace();
    String podName = peonPod.getMetadata().getName();
    PodLogs logs = new PodLogs(realK8sClient);
    InputStream is = null;
    try {
      is = logs.streamNamespacedPodLog(peonPod);
    }
    catch (ApiException ex) {
      LOGGER.warn(ex, "Exception to get pod logs [%s/%s], code[%d], error[%s].", namespace, podName, ex.getCode(), ex.getResponseBody());
    }
    catch (IOException ex) {
      LOGGER.warn(ex, "Error when get pod logs [%s/%s].", namespace, podName);
    }
    return is;
  }

  @Override
  public String waitForPodFinished(V1Pod peonPod)
  {
    String phase = getPodStatus(peonPod);
    String namespace = peonPod.getMetadata().getNamespace();
    String name = peonPod.getMetadata().getName();

    while (!phase.equalsIgnoreCase("Failed") && !phase.equalsIgnoreCase("Succeeded")) {
      try {
        LOGGER.info("Still wait for peon pod finished [%s/%s] current status is [%s]", namespace, name, phase);
        Thread.sleep(3 * 1000);
        phase = getPodStatus(peonPod);
      }
      catch (InterruptedException ex) {
        LOGGER.warn(ex, "Exception when wait for pod finished [%s/%s].", namespace, name);
      }
    }
    return phase;
  }

  @Override
  public String getPodStatus(V1Pod peonPod)
  {
    V1ObjectMeta mt = peonPod.getMetadata();
    return podClient.get(mt.getNamespace(), mt.getName()).getObject().getStatus().getPhase();
  }

  @Override
  public V1Pod getPod(V1Pod peonPod)
  {
    V1ObjectMeta mt = peonPod.getMetadata();
    return podClient.get(mt.getNamespace(), mt.getName()).getObject();
  }


  @Override
  public void deletePod(V1Pod peonPod)
  {
    V1ObjectMeta mt = peonPod.getMetadata();
    podClient.delete(mt.getNamespace(), mt.getName()).getObject();
    LOGGER.info("Peon Pod deleted : [%s/%s]", peonPod.getMetadata().getNamespace(), peonPod.getMetadata().getName());
  }

  public InputStream copyFileFromPod(V1Pod peonPod, String srcPath)
  {
    String namespace = peonPod.getMetadata().getNamespace();
    String name = peonPod.getMetadata().getName();
    String containerName = peonPod.getSpec().getContainers().get(0).getName();
    InputStream is = null;
    try {
      Copy copyRoot = new Copy(realK8sClient);

      // String namespace, String pod, String container, String srcPath
      is = copyRoot.copyFileFromPod(namespace, containerName, srcPath);
    }
    catch (ApiException | IOException ex) {
      LOGGER.warn(ex, "Exception get file from pod[%s/%s].", namespace, name);
    }
    return is;
  }
}
