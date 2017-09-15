package kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public enum ContainerIdResolver {
	
	INSTANCE;
	
	private static final String CAT_ETC_HOSTNAME = "cat /etc/hostname";
	private static final String C = "-c";
	private static final String BASH = "bash";
	
	private String containerId;
	
	private ContainerIdResolver() {
		Process p;
		try {
			p = Runtime.getRuntime().exec(new String[]{BASH,C,CAT_ETC_HOSTNAME});
			InputStream is = p.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			containerId = br.readLine();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public String getContainerId() {
		return containerId;
	}
	
	
	
}
