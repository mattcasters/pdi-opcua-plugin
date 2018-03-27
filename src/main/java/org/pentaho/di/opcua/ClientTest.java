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

package org.pentaho.di.opcua;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.UaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.client.api.nodes.Node;
import org.eclipse.milo.opcua.sdk.client.model.nodes.objects.ServerNode;
import org.eclipse.milo.opcua.sdk.client.model.nodes.variables.ServerStatusNode;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.Stack;
import org.eclipse.milo.opcua.stack.core.UaRuntimeException;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.ServerState;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.BuildInfo;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.ServerStatusDataType;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.opcua.util.KeyStoreLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class ClientTest  {

  /**
   * The URL to connect to as first argument of this program: "opc.tcp://localhost:12686/example"
   */
  private static String URL;

  /**
   * A Kettle log channel
   */
  private static LogChannelInterface log;

  private final KeyStoreLoader loader = new KeyStoreLoader();

  public static void main( String[] args ) throws Exception {

    if ( args.length != 1 ) {
      System.out.println( "ClientTest <URL>" );
      System.out.println();
      System.out.println( "Example:" );
      System.out.println( "    ClientTest 'opc.tcp://localhost:1260/UA/SomeServer:" );
      System.out.println();
      System.exit( 1 );
    }

    URL = args[ 0 ];

    KettleClientEnvironment.init();
    log = LogChannel.GENERAL;

    ClientTest clientTest = new ClientTest();
    OpcUaClient client = clientTest.createClient();

    try {
      // Connect to server URL
      //
      client.connect().get();

      // Get a ServerNode object from the server to see what it looks like
      //
      ServerNode serverNode = client.getAddressSpace().getObjectNode( Identifiers.Server, ServerNode.class ).get();

      // Read properties of the Server object...
      //
      String[] serverArray = serverNode.getServerArray().get();
      String[] namespaceArray = serverNode.getNamespaceArray().get();

      log.logBasic( "ServerArray={0}", Arrays.toString( serverArray ) );
      log.logBasic( "NamespaceArray={0}", Arrays.toString( namespaceArray ) );

      // Read the nodeValue of attribute the ServerStatus variable component
      ServerStatusDataType serverStatus = serverNode.getServerStatus().get();

      log.logBasic( "ServerStatus={0}", serverStatus );

      // Get a typed reference to the ServerStatus variable
      // component and read value attributes individually
      ServerStatusNode serverStatusNode = serverNode.serverStatus().get();
      BuildInfo buildInfo = serverStatusNode.getBuildInfo().get();
      DateTime startTime = serverStatusNode.getStartTime().get();
      DateTime currentTime = serverStatusNode.getCurrentTime().get();
      ServerState state = serverStatusNode.getState().get();

      log.logBasic( "ServerStatus.BuildInfo={0}", buildInfo );
      log.logBasic( "ServerStatus.StartTime={0}", startTime );
      log.logBasic( "ServerStatus.CurrentTime={0}", currentTime );
      log.logBasic( "ServerStatus.State={0}", state );

      browseNode("", "/", client, Identifiers.RootFolder);

      /*
      NodeId nodeId = NodeId.parse( "id=ns=1;s=EVR2.state.IN_AUTOMATIC" );
      DataValue nodeValue = client.readValue( 0, TimestampsToReturn.Both, nodeId ).get();

      log.logBasic( "Test node id = {0}", nodeId.toString());
      log.logBasic( "Test node nodeValue = {0}", nodeValue.getValue());

      List<NodeId> nodeIds = new ArrayList<NodeId>();
      client.readValues( 0, TimestampsToReturn.Both, nodeIds );
      log.logBasic( "Nr of node IDs read={0}", nodeIds.size());
*/


    } catch ( Exception e ) {
      log.logError( "Error in client test ", e );
    } finally {
      if ( client != null ) {
        try {
          client.disconnect().get();

          Stack.releaseSharedResources();
        } catch ( InterruptedException | ExecutionException e ) {
          log.logError( "Error disconnecting:", e.getMessage(), e );
        }
      } else {
        log.logError( "Error running test: no client found, not connected" );
        Stack.releaseSharedResources();
      }
    }

    log.logBasic( "Client test complete" );
    System.exit( 0 );
  }

  private static void browseNode( String indent, String path, OpcUaClient client, NodeId browseRoot) {
    try {
      List<Node> nodes = client.getAddressSpace().browse(browseRoot).get();

      for (Node node : nodes) {
        String nodeName = node.getBrowseName().get().getName();
        String nodeId = node.getNodeId().get().toParseableString();
        String nodePath = path+nodeName+"/";

        log.logBasic("{0} {1} Node={2} id={3}", nodePath, indent, nodeName, nodeId);

        // recursively browse to children
        browseNode(indent + "  ", nodePath, client, node.getNodeId().get());
      }
    } catch (InterruptedException | ExecutionException e) {
      log.logError("Browsing nodeId={0} failed: {1}", browseRoot, e.getMessage(), e);
    }
  }

  public SecurityPolicy getSecurityPolicy() {
    return SecurityPolicy.None;
  }

  public IdentityProvider getIdentityProvider() {
    return new AnonymousProvider();
  }

  public String getEndpointUrl() {
    return URL;
  }

  private OpcUaClient createClient() throws Exception {
    SecurityPolicy securityPolicy = getSecurityPolicy();

    EndpointDescription[] endpoints = UaTcpStackClient.getEndpoints( getEndpointUrl() ).get();

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
      .setIdentityProvider( getIdentityProvider() )
      .setRequestTimeout( UInteger.valueOf( 5000 ) )
      .build();

    return new OpcUaClient( config );
  }
}
