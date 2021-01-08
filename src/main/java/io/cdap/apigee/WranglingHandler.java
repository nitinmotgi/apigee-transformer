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
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceContext;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.directives.aggregates.DefaultTransientStore;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.GrammarMigrator;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.parser.ConfigDirectiveContext;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.DirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import io.cdap.wrangler.registry.UserDirectiveRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Wrangler service.
 */
public class WranglingHandler extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(WranglingHandler.class);
  private final TransientStore store = new DefaultTransientStore();
  private DirectiveRegistry composite;

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    composite = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry(),
      new UserDirectiveRegistry(context)
    );
  }

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
    String contentType = getHeader(request, "Content-Type", "text/plain; charset=UTF-8");
    String recipe = getHeader(request, "Recipe", null);

    if (recipe == null) {
      responder.sendStatus(HttpURLConnection.HTTP_BAD_REQUEST);
      return;
    }

    String content = getContent(Charset.forName("UTF-8"), request);
    if (content == null) {
      responder.sendStatus(HttpURLConnection.HTTP_BAD_REQUEST);
    }

    List<Row> rows = new ArrayList<>();
    rows.add(new Row("body", content));

    try {
      ExecutorContext context = new ServiceContext(ExecutorContext.Environment.MICROSERVICE, getContext(), store);
      RecipePipelineExecutor executor = new RecipePipelineExecutor();
      GrammarMigrator migrator = new MigrateToV2(recipe);
      String migratedGrammar = migrator.migrate();
      RecipeParser parser = new GrammarBasedParser("default", migratedGrammar, composite);
      parser.initialize(new ConfigDirectiveContext(new DirectiveConfig()));
      executor.initialize(parser, context);
      List<Row> results = executor.execute(rows);
      if (results == null) {
        results = Collections.emptyList();
      }

      List<Map<String, Object>> values = new ArrayList<>(results.size());
      for (Row row : rows) {
        Map<String, Object> value = new HashMap<>(row.width());
        for (Pair<String, Object> field : row.getFields()) {
          value.put(field.getFirst(), field.getSecond());
        }
        values.add(value);
      }
      responder.sendJson(values);
    } catch (DirectiveParseException e) {
      responder.sendJson(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    } catch (RecipeException e) {
      responder.sendJson(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendJson(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    }
  }


}
