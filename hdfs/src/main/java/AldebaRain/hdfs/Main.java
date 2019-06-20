package AldebaRain.hdfs;

import AldebaRain.hdfs.datanode.DataNodeServer;
import AldebaRain.hdfs.namenode.NameNodeServer;

/**
 * Allow the servers to be invoked from the command-line. The jar is built with
 * this as the <code>Main-Class</code> in the jar's <code>MANIFEST.MF</code>.
 * 
 */
public class Main {

	/** 块大小，为64MB */
	//public static int BlockSize = 64;

	/** 副本数 */
	//public static int CopyNum = 3; 

	/** Get方法中展示某一块时的字节数（最好小于BlockSize） */
	//public static int ShowNum = 32;

	public static void main(String[] args) {

		String serverName = "NO-VALUE";

		switch (args.length) {
		case 2:
			// Optionally set the HTTP port to listen on, overrides value in the <server-name>-server.yml file
			System.setProperty("server.port", args[1]);
		case 1:
			serverName = args[0].toLowerCase();
			break;
		default:
			usage();
			return;
		}

		if (serverName.equals("registration") || serverName.equals("reg")) {
			RegistrationServer.main(args);
		} 
		else if (serverName.equals("namenode") || serverName.equals("name")) {
			NameNodeServer.main(args);
		} 
		else if (serverName.equals("datanode") || serverName.equals("data")) {
			DataNodeServer.main(args);
		} 
		else {
			System.out.println("Unknown server type: " + serverName);
			usage();
		}
	}

	protected static void usage() {
		System.out.println("Usage: mvn spring-boot:run -P<server-name> [-Dserver.port=<server-port>]");
		System.out.println("     where server-name is 'registration' or 'namenode' or 'datanode' and server-port > 1024");
	}
}
