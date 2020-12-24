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

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

public class K8sMiddleManagerModule implements DruidModule
{
  private static final String INDEXER_RUNNER_MODE_K8S = "k8s";

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.middlemanager.k8s", K8sMiddleManagerConfig.class);
    final MapBinder<String, TaskRunner> biddy = PolyBind.optionBinder(
            binder,
            Key.get(TaskRunner.class)
    );
    biddy.addBinding(INDEXER_RUNNER_MODE_K8S).to(K8sForkingTaskRunner.class);
    binder.bind(K8sForkingTaskRunner.class).in(LazySingleton.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }
}
