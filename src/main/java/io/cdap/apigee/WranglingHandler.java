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

import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Wrangler service.
 */
public class WranglingHandler extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(WranglingHandler.class);

  @Nullable
  public String getContent(Charset charset, HttpServiceRequest request) {
    ByteBuffer content = request.getContent();
    if (content != null && content.hasRemaining()) {
      return charset.decode(content).toString();
    }
    return null;
  }

  @Nullable
  public String getHeader(HttpServiceRequest request, String name, String defaultValue) {
    String header = request.getHeader(name);
    return header == null ? defaultValue : header;
  }

  @POST
  @Path("transform")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void execute(HttpServiceRequest request, HttpServiceResponder responder) {
    String contentType = getHeader(request, "Content-type", "text/plain");
    String recipe = getHeader(request, "Recipe", null);

    if (recipe == null) {
      responder.sendStatus(HttpURLConnection.HTTP_BAD_REQUEST);
      return;
    }

    String content = getContent(Charset.forName(contentType), request);
    if (content == null) {
      responder.sendStatus(HttpURLConnection.HTTP_BAD_REQUEST);
    }
    
    responder.sendStatus(HttpURLConnection.HTTP_OK);
  }

}
