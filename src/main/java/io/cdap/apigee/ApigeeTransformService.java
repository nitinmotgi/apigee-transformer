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

import io.cdap.cdap.api.service.AbstractService;

/**
 * Transform service.
 */
public class ApigeeTransformService extends AbstractService {

  @Override
  protected void configure() {
    setName("apigee-tx-service");
    addHandler(new WranglingHandler());
  }
}
