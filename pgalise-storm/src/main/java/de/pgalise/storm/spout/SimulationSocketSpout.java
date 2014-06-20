package de.pgalise.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Tuple;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class SimulationSocketSpout extends BaseRichSpout {

  private static final long serialVersionUID = 1L;
  private final static Logger LOGGER = LoggerFactory.getLogger(SimulationSocketSpout.class);
  private SpoutOutputCollector outputCollector;
  private boolean running = true;
  private Socket socket;
  private DataInputStream dataInputStream;  
  private final Thread socketReaderThread = new Thread() {
    @Override
    public void run() {
      while (running) {
        try {
          Long sensorId = dataInputStream.readLong();
          processBytes(sensorId);
        }catch(IOException ex) {
          LOGGER.debug("(see nested exception)", ex);
        }
      }
    }
  };
  private final Queue<Tuple> tuples = new LinkedList<>();

  public SimulationSocketSpout(InetSocketAddress inetSocketAddress) throws IOException {
    socket = new Socket(inetSocketAddress.getAddress(), inetSocketAddress.getPort());
    this.dataInputStream = new DataInputStream(socket.getInputStream());
  }
  
  /**
   * starts socket reading thread
   */
  public void initialize() {
    this.socketReaderThread.start();    
  }

  protected SpoutOutputCollector getOutputCollector() {
    return outputCollector;
  }

  protected DataInputStream getDataInputStream() {
    return dataInputStream;
  }
  
  /**
   * implement how to read primitives from {@link #getDataInputStream() }. Returned values are enqueued and passed to {@link OutputCollector} at the next invokation of {@link #nextTuple() } automatically.
   * @param sensorId
   * @return the {@link Tuple} constructed from primitives
   */
  public abstract Tuple processBytes(Long sensorId);

  @Override
  public void nextTuple() {
    if(!tuples.isEmpty()) {
      synchronized(tuples) {
        outputCollector.emit(tuples.poll().getValues());
      }
    }
  }

  @Override
  public void open(Map map, TopologyContext tc, SpoutOutputCollector soc) {
    this.outputCollector = soc;
  }

}
