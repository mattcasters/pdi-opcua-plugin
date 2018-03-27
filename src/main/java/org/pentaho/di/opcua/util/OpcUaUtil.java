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

package org.pentaho.di.opcua.util;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.opcua.connection.OpcUaConnection;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

import java.util.Arrays;

public class OpcUaUtil {

  private static final KeyStoreLoader loader = new KeyStoreLoader();

  /**
   * Create a new client connection to an OPC UA server
   *
   * @param log The logging channel to use
   * @param endPointUrl The URL to the endpoint
   * @param securityPolicy
   * @param identityProvider
   * @return The client (not connected)
   * @throws Exception
   */
  public static OpcUaClient createClient(
    LogChannelInterface log,
    String endPointUrl,
    SecurityPolicy securityPolicy,
    IdentityProvider identityProvider) throws Exception {

    EndpointDescription[] endpoints = UaTcpStackClient.getEndpoints( endPointUrl ).get();

    EndpointDescription endpoint = Arrays.stream( endpoints )
      .filter( e -> e.getSecurityPolicyUri().equals( securityPolicy.getSecurityPolicyUri() ) )
      .findFirst().orElseThrow( () -> new Exception( "no desired endpoints returned" ) );

    log.logBasic( "Using endpoint: "+ endpoint.getEndpointUrl()+" security policy: "+securityPolicy );

    loader.load();

    OpcUaClientConfig config = OpcUaClientConfig.builder()
      .setApplicationName( LocalizedText.english( "opc-ua Kettle client test" ) )
      .setApplicationUri( "urn:kettle:milo:client:test" )
      .setCertificate( loader.getClientCertificate() )
      .setKeyPair( loader.getClientKeyPair() )
      .setEndpoint( endpoint )
      .setIdentityProvider( identityProvider )
      .setRequestTimeout( UInteger.valueOf( 5000 ) )
      .build();

    return new OpcUaClient( config );
  }

  public static MetaStoreFactory<OpcUaConnection> getFactory(IMetaStore metaStore) {
    return new MetaStoreFactory<OpcUaConnection>( OpcUaConnection.class, metaStore, PentahoDefaults.NAMESPACE );
  }

}
