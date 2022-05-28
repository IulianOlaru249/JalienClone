/**
 * 
 */
package alien.websockets;

import java.net.InetAddress;

/**
 * @author costing
 * @since Feb 2, 2021
 */
public class WebSocketInfo {
	/**
	 * Account name registered on this connection
	 */
	public final String account;
	
	/**
	 * IP address where this connection was created from
	 */
	public final InetAddress host;
	
	/**
	 * TCP port on the client side
	 */
	public final int port;
	
	/**
	 * When the connection was established
	 */
	public final long connectedTimestamp;
	
	/**
	 * And last time it was known to be active
	 */
	public final long lastActiveTimestamp;

	/**
	 * Simple constructor
	 * 
	 * @param account
	 * @param host
	 * @param port
	 * @param connectedTimestamp
	 * @param lastActiveTimestamp
	 */
	public WebSocketInfo(final String account, final InetAddress host, final int port, final long connectedTimestamp, final long lastActiveTimestamp) {
		this.account = account;
		this.host = host;
		this.port = port;
		this.connectedTimestamp = connectedTimestamp;
		this.lastActiveTimestamp = lastActiveTimestamp;
	}
}
