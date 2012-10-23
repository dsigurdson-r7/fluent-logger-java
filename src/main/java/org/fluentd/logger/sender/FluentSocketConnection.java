/**
 * FluentSocketConnection - originally inspired by the RawSocketSender from the 
 * fluent-logger project. 
 */
package org.fluentd.logger.sender;

import java.io.OutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.logging.Level;
import org.msgpack.MessagePack;

/**
 * Connection to Fluentd over a raw socket. Will block attempting to make a connection to the Fluentd server
 * and will retry the connection on any socket failures.
 */
public class FluentSocketConnection {
	private static final java.util.logging.Logger Log =
	        java.util.logging.Logger.getLogger(FluentSocketConnection.class.getName());
	
	private String m_tag;
	private String m_host;
	private int m_port;
	private Socket m_socket;
	private OutputStream m_outputStream;
	
	private MessagePack m_messagePack = new MessagePack();
	
	public FluentSocketConnection(String tag, String host, int port) {
		m_messagePack.register(Event.class, Event.EventTemplate.INSTANCE);
		m_tag = tag;
		m_host = host;
		m_port = port;
	}
	
	public void log(String label, Map<String, Object> data) {
		// Create the new event
		Event event = new Event(m_tag + "." + label, System.currentTimeMillis() / 1000, data);
		byte[] bytes = null;
		
		// Attempt to get the bytes for the event						
		try {			
			bytes = m_messagePack.write(event);
		} catch (IOException e) {
			// There was a problem serializing the data - we will simply drop this message due to that
			Log.severe("Cannot serialize event: " + event);
			return;
		}
		
		// And now go and send the bytes
		send(bytes);
	}
			
	/**
	 * Sends the given bytes worth of data over the fluent socket connection 
	 */
	private synchronized void send(byte[] bytes) {
		int sleepMilliseconds = 1;
		
		while (true) {
			// Attempt to send the given bytes
			try {
				OutputStream outputStream = getOutputStream();
				outputStream.write(bytes);
				break;				
			} catch (UnknownHostException e) {				
				Log.log(Level.SEVERE, "UnknownHostException in send", e);
				e.printStackTrace();
				closeSocket();
			} catch (IOException e) { 
				Log.log(Level.SEVERE, "IOException in send", e);
				e.printStackTrace();
				closeSocket();
			}
			
			// If we made it here then there was an error - retry later
			try {
				Thread.sleep(sleepMilliseconds);
				sleepMilliseconds = Math.min(60000, 2 * sleepMilliseconds);
			} catch (InterruptedException e) {
				// break out of the while loop
				break;
			}			
		}
	}
	
	private OutputStream getOutputStream() throws UnknownHostException, IOException {
		if (m_outputStream == null) {
			m_socket = new Socket(m_host, m_port);
			m_outputStream = m_socket.getOutputStream();			
		}
		
		return m_outputStream;
	}
	
	private void closeSocket() {
		try {
			// Try to close the output stream if it exists
			if (m_outputStream != null) {
				m_outputStream.close();			
			}
			
			if (m_socket != null) {
				m_socket.close();
			}
		}	
		catch (IOException e) {
			Log.log(Level.SEVERE, "IOException in close", e);
			e.printStackTrace();
		}
		finally {
			m_outputStream = null;
			m_socket = null;
		}
	}
	
	/**
	 * Closes the Fluentd connection and flushes any remaining data.
	 */
	public synchronized void close() {
		closeSocket();
	}
}
