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

import java.util.List;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.opcua.connection.OpcUaConnection;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.metastore.util.PentahoDefaults;
import org.w3c.dom.Node;

@Step(
  id = "OpcUaInput",
  description = "Reads from an OPC UA server",
  name = "OPC UA Input",
  image = "ui/images/TIP.svg",
  categoryDescription = "Input"
)
public class OpcUaMeta extends BaseStepMeta implements StepMetaInterface {

  private static final String TAG_OPC_UA_CONNECTION = "opc_ua_connection";

  private String opcUaConnectionName;

  public OpcUaMeta() {
    super();
  }

  @Override
  public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                        VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

    // load OPC UA connection from the shared metastore objects...
    //
    String connectionName = space.environmentSubstitute( opcUaConnectionName );
    try {

      MetaStoreFactory<OpcUaConnection> factory = new MetaStoreFactory<OpcUaConnection>(OpcUaConnection.class, metaStore, PentahoDefaults.NAMESPACE );
      OpcUaConnection connection = factory.loadElement( connectionName );
      if (connection==null) {
        throw new KettleException("Unable to find OPC UA connection with name '"+connectionName+"'");
      }

      // All is fine, we could get metadata from the connection but we just wanted to test existence.
      //
    } catch(Exception e) {
      throw new KettleStepException("Unable to connect to OPC UA server '"+connectionName+"'", e);
    }

    ValueMetaInterface serverState = new ValueMetaString("ServerState");
    inputRowMeta.addValueMeta(serverState);

    ValueMetaInterface serverCurrentTime = new ValueMetaDate("ServerCurrentTime");
    inputRowMeta.addValueMeta(serverCurrentTime);
  }

  @Override
  public String getXML() throws KettleException {
    StringBuilder xml = new StringBuilder();

    xml.append( XMLHandler.addTagValue( TAG_OPC_UA_CONNECTION, opcUaConnectionName ) );

    return xml.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    try {

      opcUaConnectionName = XMLHandler.getTagValue( stepnode, TAG_OPC_UA_CONNECTION );

    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load execute test step details", e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {

    rep.saveStepAttribute( id_transformation, id_step, TAG_OPC_UA_CONNECTION, opcUaConnectionName );
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {

    opcUaConnectionName = rep.getStepAttributeString( id_step, TAG_OPC_UA_CONNECTION );
  }


  @Override
  public StepInterface getStep(StepMeta meta, StepDataInterface data, int copy, TransMeta transMeta, Trans trans) {
    return new OpcUa(meta, data, copy, transMeta, trans);
  }

  @Override
  public StepDataInterface getStepData() {
    return new OpcUaData();
  }

  @Override
  public String getDialogClassName() {
    return OpcUaDialog.class.getName();
  }

  @Override
  public void setDefault() {
    opcUaConnectionName = "";
  }

  public String getOpcUaConnectionName() {
    return opcUaConnectionName;
  }

  public void setOpcUaConnectionName( String opcUaConnectionName ) {
    this.opcUaConnectionName = opcUaConnectionName;
  }
}
