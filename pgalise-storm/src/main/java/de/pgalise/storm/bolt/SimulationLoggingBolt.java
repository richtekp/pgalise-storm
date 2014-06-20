/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package de.pgalise.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author richter
 */
public class SimulationLoggingBolt extends BaseRichBolt {
  private static final long serialVersionUID = 1L;
  private final static Logger LOGGER = LoggerFactory.getLogger(SimulationLoggingBolt.class);

  public SimulationLoggingBolt() {
  }

  @Override
  public void execute(Tuple input) {
    LOGGER.debug(input.getLong(0).toString());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id"));
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    
  }
  
}

