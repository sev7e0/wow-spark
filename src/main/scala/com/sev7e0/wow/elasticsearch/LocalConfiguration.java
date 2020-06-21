package com.sev7e0.wow.elasticsearch;

/**
 * Title:  LocalConfiguration.java
 * description: es 本地配置
 *
 * @author sev7e0
 * @version 1.0
 * @since 2020-06-21 15:46
 **/

public interface LocalConfiguration {


	// 需要连接的 es 节点（不需要配置全部节点，默认会自动发现其他可用节点）；
	String ES_NODES = "localhost";
	// es端口
	String ES_PORT = "9200";
	// 认证用户名
	String ES_NET_HTTP_AUTH_USER = "";
	// 密码
	String ES_NET_HTTP_AUTH_PASS = "";
	// 默认为 false，设置为 true 之后，会关闭节点的自动 discovery，只使用 es.nodes 声明的
	// 节点进行数据读写操作；如果你需要通过域名进行数据访问，则设置该选项为 true，否则请务必设置为 false；
	String ES_NODES_WAN_ONLY = "true";
	// 当索引不存在时自动创建索引
	String ES_INDEX_AUTO_CREATE = "true";
	// 默认为true，自动发现可用节点
	String ES_NODES_DISCOVERY = "false";

}
