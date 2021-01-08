/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.apigee;

import edu.emory.mathcs.backport.java.util.Collections;
import io.cdap.cdap.api.service.http.HttpServiceContext;
import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.common.NoopMetrics;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.TransientStore;

import java.net.URL;
import java.util.Map;

/**
 * Service Context.
 */
public class ServiceContext implements ExecutorContext {
  private final Environment environment;
  private final HttpServiceContext context;
  private final TransientStore store;

  public ServiceContext(Environment environment, HttpServiceContext context, TransientStore store) {
    this.environment = environment;
    this.context = context;
    this.store = store;
  }

  @Override
  public Environment getEnvironment() {
    return environment;
  }

  @Override
  public String getNamespace() {
    return "default";
  }

  @Override
  public StageMetrics getMetrics() {
    return NoopMetrics.INSTANCE;
  }

  @Override
  public String getContextName() {
    return context.getSpecification().getName();
  }

  @Override
  public Map<String, String> getProperties() {
    return Collections.emptyMap();
  }

  @Override
  public URL getService(String applicationId, String serviceId) {
    return context.getServiceURL(applicationId, serviceId);
  }

  @Override
  public TransientStore getTransientStore() {
    return store;
  }

  @Override
  public <T> Lookup<T> provide(String s, Map<String, String> map) {
    return null;
  }
}
