/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package de.pgalise.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Tuple;
import de.pgalise.simulation.operationCenter.internal.model.sensordata.SensorData;
import gnu.cajo.invoke.Remote;
import gnu.cajo.utils.ItemServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Waits for PGALISE (has to be extended appropriately) to invoke 
 * {@link ControlCenterConstants#XML_RPC_METHOD_NAME}
 * @author richter
 * @param <S> The type of {@link SensorData} which is expected  to be pushed 
 * over XML-RPC
 */
public abstract class SimulationXMLRPCSpout<S extends SensorData> extends BaseRichSpout  {
  private static final long serialVersionUID = 1L;
  private Queue<S> queue = new LinkedList<>();
  private Queue<Tuple> tuples = new LinkedList<>();
  private InetAddress xMLRPCAddress;
  private int xMLRPCPort;  
  private SpoutOutputCollector outputCollector;

  /**
   * initializes a <tt>SimulationXMLRPCSpout</tt> which has to be 
   * started with {@link #start() }
   * @param xMLRPCAddress
   * @param xMLRPCPort
   * @throws UnknownHostException 
   */
  public SimulationXMLRPCSpout(InetAddress xMLRPCAddress, int xMLRPCPort) throws UnknownHostException {
    this.xMLRPCAddress =  xMLRPCAddress;
    this.xMLRPCPort = xMLRPCPort;
  }
  
  public void start() throws IOException {    
    Remote.config(
      xMLRPCAddress.getHostName(), //serverHost
      xMLRPCPort, //serverPort
      null, //clientHost @TODO: ??
      0 //clientPort @TODO: ??
    );
    ItemServer.bind(this, "einName");
  }
  
  /**
   * to be invoked with XML-RPC. @TODO: specify some constraints or 
   * that there're none (order in list, etc.)
   * 
   * invokes {@link #processData() }
   * @param sensorDatas the update sent from PGALISE
   */
  public void updateSimulation(List<S> sensorDatas) {
    queue.addAll(sensorDatas);
    processData();
  }

  protected Queue<S> getQueue() {
    return queue;
  }
  
  /**
   * process the content of sensor data queue ({@link #getQueue() }) by transforming it into {@link Tuple}s 
   * @return the processing result
   */
  public abstract Tuple processData();

  @Override
  public void open(Map map, TopologyContext tc, SpoutOutputCollector soc) {
    this.outputCollector = soc;
  }

  @Override
  public void nextTuple() {
    if(!tuples.isEmpty()) {
      synchronized(tuples) {
        outputCollector.emit(tuples.poll().getValues());
      }
    }
  }
  
}
