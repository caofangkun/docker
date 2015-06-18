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

	private static void printUsage() {
		System.out.println("Envs:");
		System.out.println(" STORM_ZOOKEEPER_SERVERS");
		System.out.println(" STORM_ZOOKEEPER_PORT");
		System.out.println(" STORM_ZOOKEEPER_ROOT");
		System.out.println(" SUPERVISOR_SLOTS_NUM");
		System.out.println(" GAIA_PORT_MAPPING");
		System.out.println(" GAIA_HOST_IP");

		System.out.println("Usage:");
		System.out
				.println(" java -jar generator.jar  $STORM_CONF_DIR/storm.yaml");
		System.exit(-1);
	}

	public static void main(String[] args) {

		if (args == null || args.length == 0) {
			System.out.println("Should privide storm.yaml path at least!");
			printUsage();
		}
		String stormYaml = args[0];
//		stormYaml = "/home/kuncao/workspace/storm-core-java/conf/storm.yaml";
		System.out.println("get storm.yaml in: " + stormYaml);
		File configFile = new File(stormYaml);
		InputStream inputStream = null;
		Yaml yaml = null;
		Map<String, Object> formatterConfig = null;
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
		} // storm.local.hostname
		String gaiaHostIp = System.getenv("GAIA_HOST_IP");
		if (gaiaHostIp != null) {
			formatterConfig.put("storm.local.hostname", gaiaHostIp);
			System.out.println("set storm.local.hostname: " + gaiaHostIp);
		}
		// "{\"6700/tcp\":[{\"HostIP\":\"1.1.1.1.\", \"HostPort\":\"49252\"}],\"6701/tcp\":[{\"HostIP\":\"1.1.1.1.\", \"HostPort\":\"49254\"}]}";
		// supervisor.slots.port
		String supervisorSlotsNum = System.getenv("SUPERVISOR_SLOTS_NUM");
		int slots = 0;
		if (supervisorSlotsNum == null
				|| Integer.valueOf(supervisorSlotsNum) >= 4) {
			slots = 1;
		} else {
			slots = Integer.valueOf(supervisorSlotsNum);
		}
		System.out.println("set supervisorSlotsNum: " + supervisorSlotsNum);

		String json = System.getenv("GAIA_PORT_MAPPING");
		// json =
		// "{\"80/tcp\": [{\"HostIp\": \"0.0.0.0\",\"HostPort\": \"49226\"}],\"6703/tcp\": [{\"HostIp\": \"0.0.0.0\", \"HostPort\": \"49224\"}], \"6702/tcp\": [{\"HostIp\": \"0.0.0.0\", \"HostPort\": \"49223\"}], \"36000/tcp\": [{\"HostIp\": \"0.0.0.0\",\"HostPort\": \"49227\"}], \"6701/tcp\": [{\"HostIp\": \"0.0.0.0\",\"HostPort\": \"49222\"}], \"6700/tcp\": [], \"7627/tcp\": []}";
		if (json != null) {
			System.out.println("get GAIA_PORT_MAPPING: " + json);
			json = json.replaceAll("'", "");
			ObjectMapper jsonMapper = new ObjectMapper();

			Map<String, List<HostToPort>> tmp;
			List<String> v2R = new ArrayList<String>();
			try {
				tmp = jsonMapper.readValue(json.toLowerCase(),
						new TypeReference<Map<String, List<HostToPort>>>() {
						});
				for (Map.Entry<String, List<HostToPort>> entry : tmp.entrySet()) {
					Integer virualPort = Integer.valueOf(entry.getKey().split(
							"/")[0]);
					if (!entry.getValue().isEmpty()) {
						Integer realPort = Integer.valueOf(entry.getValue()
								.get(0).getHostport());
						v2R.add(virualPort + ":" + realPort);
					}
				}
				formatterConfig.put("supervisor.slots.ports", v2R);
				System.out.println("set supervisor.slots.ports: "
						+ v2R.toString());

			} catch (Exception e) {
				System.out.println("Error: " + e.getMessage());
			}
		} else {
			System.out.println("GAIA_PORT_MAPPING is null");
		}
		// storm.zookeper.servers
		String stormZookeeperSevrvers = System
				.getenv("STORM_ZOOKEEPER_SERVERS");
		if (stormZookeeperSevrvers != null) {
			List<String> servers = new ArrayList<String>();
			for (String server : stormZookeeperSevrvers.split(":")) {
				servers.add(server);
			}
			formatterConfig.put("storm.zookeeper.servers", servers);
			System.out.println("set storm.zookeeper.servers: "
					+ servers.toString());

		} else {
			System.out.println("Please set STORM_ZOOKEEPER_SERVERS");
		}

		String stormZookeeperPort = System.getenv("STORM_ZOOKEEPER_PORT");
		if (stormZookeeperPort != null) {
			formatterConfig.put("storm.zookeeper.port", stormZookeeperPort);
			System.out.println("set storm.zookeeper.port: "
					+ stormZookeeperPort);
		} else {
			System.out.println("Please set STORM_ZOOKEEPER_PORT");
		}

		String stormZookeeperRoot = System.getenv("STORM_ZOOKEEPER_ROOT");
		if (stormZookeeperRoot != null) {
			formatterConfig.put("storm.zookeeper.root", stormZookeeperRoot);
			System.out.println("set storm.zookeeper.root: "
					+ stormZookeeperRoot);

		} else {
			System.out.println("Please set STORM_ZOOKEEPER_ROOT");
		}
		// //
		try {
			yaml = new Yaml(new SafeConstructor());
			PrintWriter writer = new PrintWriter(new File(stormYaml));
			writer.write(yaml.dump(formatterConfig));
			writer.close();
			System.out.println("generate new storm.yaml in: " + stormYaml);
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		}

	}
}
