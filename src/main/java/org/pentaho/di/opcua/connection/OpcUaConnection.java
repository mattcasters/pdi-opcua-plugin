/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.opcua.connection;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.opcua.util.OpcUaUtil;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

@MetaStoreElementType(
  name = "OpcUaConnection",
  description = "OPC UA Connection"
)
public class OpcUaConnection {
  private String name;

  @MetaStoreAttribute
  private String url;

  public OpcUaConnection() {
  }

  public OpcUaConnection( String name, String url ) {
    this();
    this.name = name;
    this.url = url;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl( String url ) {
    this.url = url;
  }

  public void test( VariableSpace space, SecurityPolicy securityPolicy, IdentityProvider identityProvider) throws Exception {
    String realUrl = space.environmentSubstitute(url);
    OpcUaClient client = OpcUaUtil.createClient( LogChannel.GENERAL, url, securityPolicy, identityProvider );
    try {
      client.connect().get();
    } finally {
      client.disconnect();
    }
  }

}
