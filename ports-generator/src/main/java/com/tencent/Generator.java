package com.tencent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public class Generator {
	private static Yaml yaml = null;
	private static Map<String, Object> formatterConfig = null;

	private static void printUsage() {
		System.out.println("Envs:");
		System.out.println(" STORM_ZOOKEEPER_SERVERS");
		System.out.println(" STORM_ZOOKEEPER_PORT");
		System.out.println(" STORM_ZOOKEEPER_ROOT");
		System.out.println(" SUPERVISOR_SLOTS_NUM");
		System.out.println(" GAIA_PORT_MAPPING");

		System.out.println("Usage:");
		System.out
				.println(" java -jar Generator.jar  com.tencent.Generator $STORM_CONF_DIR/storm.yaml");
		System.exit(-1);
	}

	public static void main(String[] args) {

		if (args == null || args.length == 0) {
			System.out.println("Should privide storm.yaml path at least!");
			printUsage();
		}

		String stormYaml = args[0];
		readStormYaml(stormYaml);
		// storm.local.hostname
		configStormLocalHostName();
		// "{\"6700/tcp\":[{\"HostIP\":\"1.1.1.1.\", \"HostPort\":\"49252\"}],\"6701/tcp\":[{\"HostIP\":\"1.1.1.1.\", \"HostPort\":\"49254\"}]}";
		// supervisor.slots.port
		configSupervisorSlotsPort();

		configStormZookeeper();

		genernatorNewYaml(stormYaml);

	}

	private static void configStormZookeeper() {
		String stormZookeeperSevrvers = System
				.getenv("STORM_ZOOKEEPER_SERVERS");
		if (stormZookeeperSevrvers != null) {
			List<String> servers = new ArrayList<String>();
			for (String server : stormZookeeperSevrvers.split(":")) {
				servers.add(server);
			}
			formatterConfig.put("storm.zookeeper.servers", servers);
		} else {
			System.out.println("Please set STORM_ZOOKEEPER_SERVERS");
			printUsage();
		}

		String stormZookeeperPort = System.getenv("STORM_ZOOKEEPER_PORT");
		if (stormZookeeperPort != null) {
			formatterConfig.put("storm.zookeeper.port", stormZookeeperPort);
		} else {
			System.out.println("Please set STORM_ZOOKEEPER_PORT");
			printUsage();
		}

		String stormZookeeperRoot = System.getenv("STORM_ZOOKEEPER_ROOT");
		if (stormZookeeperRoot != null) {
			formatterConfig.put("storm.zookeeper.root", stormZookeeperRoot);
		} else {
			System.out.println("Please set STORM_ZOOKEEPER_ROOT");
			printUsage();
		}
	}

	private static void genernatorNewYaml(String stormYaml) {
		try {
			yaml = new Yaml(new SafeConstructor());
			PrintWriter writer = new PrintWriter(new File(stormYaml));
			writer.write(yaml.dump(formatterConfig));
			writer.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
			printUsage();
		}
	}

	private static void configSupervisorSlotsPort() {
		String supervisorSlotsNum = System.getenv("SUPERVISOR_SLOTS_NUM");
		int slots = 0;
		if (supervisorSlotsNum == null
				|| Integer.valueOf(supervisorSlotsNum) >= 4) {
			slots = 1;
		} else {
			slots = Integer.valueOf(supervisorSlotsNum);
		}
		String json = System.getenv("GAIA_PORT_MAPPING");
		if (json != null) {
			json = json.replaceAll("'", "");
			ObjectMapper jsonMapper = new ObjectMapper();

			Map<String, List<HostToPort>> tmp;
			List<String> v2R = new ArrayList<String>();
			try {
				tmp = jsonMapper.readValue(json.toLowerCase(),
						new TypeReference<Map<String, List<HostToPort>>>() {
						});
				int cnt = 0;
				for (Map.Entry<String, List<HostToPort>> entry : tmp.entrySet()) {
					Integer virualPort = Integer.valueOf(entry.getKey().split(
							"/")[0]);
					Integer realPort = Integer.valueOf(entry.getValue().get(0)
							.getHostport());
					cnt++;
					if (cnt <= slots) {
						v2R.add(virualPort + ":" + realPort);
					}
				}
				formatterConfig.put("supervisor.slots.ports", v2R);
			} catch (Exception e) {
				System.err.println(e.getMessage());
				printUsage();
			}
		} else {
			System.err.println("GAIA_PORT_MAPPING is null");
			printUsage();
		}
	}

	private static void configStormLocalHostName() {
		String gaiaHostIp = System.getenv("GAIA_HOST_IP");
		if (gaiaHostIp != null) {
			formatterConfig.put("storm.local.hostname", gaiaHostIp);
		}
	}

	private static void readStormYaml(String stormYaml) {
		File configFile = new File(stormYaml);
		InputStream inputStream = null;
		try {
			if (configFile.exists()) {
				inputStream = new FileInputStream(configFile);
			}
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
			printUsage();
		}

		if (null != inputStream) {
			yaml = new Yaml(new SafeConstructor());
			Map ret = (Map) yaml.load(new InputStreamReader(inputStream));
			if (null != ret) {
				formatterConfig = new HashMap(ret);
			}
		}
	}
}
