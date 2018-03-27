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

package org.pentaho.di.opcua.step;

import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.model.nodes.objects.ServerNode;
import org.eclipse.milo.opcua.sdk.client.model.nodes.variables.ServerStatusNode;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.structured.ServerStatusDataType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.opcua.connection.OpcUaConnection;
import org.pentaho.di.opcua.util.OpcUaUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;

public class OpcUa extends BaseStep implements StepInterface {

  public OpcUa( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    OpcUaData data = (OpcUaData) sdi;

    data.connection = null;

    return super.init( smi, sdi );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    OpcUaMeta meta = (OpcUaMeta) smi;
    OpcUaData data = (OpcUaData) sdi;

    if ( first ) {
      first = false;

      connectToServer( this, log, meta, data );

      data.outputRowMeta = new RowMeta();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, data.store );
    }

    try {
      ServerNode serverNode = data.client.getAddressSpace().getObjectNode( Identifiers.Server, ServerNode.class ).get();

      ServerStatusDataType serverStatus = serverNode.getServerStatus().get();
      ServerStatusNode serverStatusNode = serverNode.serverStatus().get();

      String serverState = serverStatus.toString();
      DateTime serverCurrentTime = serverStatusNode.getCurrentTime().get();

      // Get 1 row of data from server: server state and time
      //
      Object[] row = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
      int index = 0;
      row[ index++ ] = serverState;
      row[ index++ ] = serverCurrentTime.getJavaDate();

      putRow( data.outputRowMeta, row );
    } catch(Exception e) {
      throw new KettleException( "Unable to get information from server '"+data.connection.getName()+"'", e );
    }

    // One row and we're done...
    //
    setOutputDone();
    return false;

  }

  /**
   * Load server from metastore, create client, connect to server
   *
   * @param meta
   * @param data
   */
  public void connectToServer( VariableSpace space, LogChannelInterface log, OpcUaMeta meta, OpcUaData data) throws KettleException {
    // load OPC UA connection from the shared metastore objects...
    //
    String connectionName = space.environmentSubstitute( meta.getOpcUaConnectionName());
    try {
      MetaStoreFactory<OpcUaConnection> factory = new MetaStoreFactory<OpcUaConnection>(OpcUaConnection.class, getAMetaStore(), PentahoDefaults.NAMESPACE );
      data.connection = factory.loadElement( connectionName );
      if (data.connection==null) {
        throw new KettleException("Unable to find OPC UA connection with name '"+connectionName+"'");
      }
    } catch(Exception e) {
      throw new KettleStepException("Unable to connect to OPC UA server '"+connectionName+"'", e);
    }

    // Create client
    //
    try {
      data.client = OpcUaUtil.createClient( log, data.connection.getUrl(), SecurityPolicy.None, new AnonymousProvider() );
    } catch ( Exception e ) {
      throw new KettleStepException("Unable to create new OPC UA client for server '"+connectionName+"'", e);
    }

    // Connect to the server
    //
    try {
      data.client.connect().get();
    } catch(Exception e) {
      try {
        data.client = OpcUaUtil.createClient( log, data.connection.getUrl(), SecurityPolicy.None, new AnonymousProvider() );
      } catch ( Exception ex ) {
        throw new KettleStepException("Unable to connect to OPC UA server '"+connectionName+"'", ex);
      }
    }
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    OpcUaData data = (OpcUaData) sdi;

    if (data.client!=null) {
      data.client.disconnect();
    }
    super.dispose( smi, sdi );
  }

  private IMetaStore getAMetaStore() throws MetaStoreException {
    IMetaStore store = metaStore;
    // during exec of data service, metaStore is not passed down
    //
    if ( store == null ) {
      store = getTrans().getMetaStore();
    }
    if ( store == null ) {
      store = getTransMeta().getMetaStore();
    }
    if ( store == null && getTrans().getParentTrans() != null ) {
      store = getTrans().getParentTrans().getMetaStore();
    }
    if ( store == null ) {
      log.logError( "Unable to find the metastore, locating it ourselves..." );
      if ( repository != null ) {
        store = repository.getMetaStore();
      } else {
        store = MetaStoreConst.openLocalPentahoMetaStore();
      }
    }
    return store;
  }
}
