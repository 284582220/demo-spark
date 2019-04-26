package com.ygj;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.fastjson.JSONObject;

public class ElasticSearchTools {

	private static final Logger log = LoggerFactory.getLogger(ElasticSearchTools.class);

	// 创建私有对象
	protected static RestHighLevelClient client;

	public ElasticSearchTools() {
		init_clinet();
	}

	public static RestHighLevelClient getClinet() {
		return client;
	}

	public void closeClinet() {
		try {
			client.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * java 客户端连接es
	 */
	public static void init_clinet() {
		Yaml yaml = new Yaml();
//		URL url = ElasticSearchTools.class.getClassLoader().getResource("es_config.yaml");
//		if(url == null) {
//			System.out.println("client_url is null");
//		}
//		InputStream in = ElasticSearchTools.class.getClassLoader().getResourceAsStream("es_config.yaml");
//		if(in == null) {
//			System.out.println("init_clinet is null");
//		}
		// System.out.println("url.getFile()="+url.getFile());
//		Map<String, Object> es_map = yaml.load(in);
//		System.out.println("es_host:"+es_map.get("es_host"));
//		String[] es_host = ((String) es_map.get("es_host")).split(",");
		String[] es_host = {"es-dev01.yingzi.com","es-dev02.yingzi.com","es-dev03.yingzi.com"};
		int port = 9200;
		HttpHost[] list = new HttpHost[es_host.length];
		for (int i = 0; i < es_host.length; i++) {
			HttpHost one = new HttpHost(es_host[i], port, "http");
			list[i] = one;
		}
		RestClientBuilder restClientBuilder = RestClient.builder(list);
		// 下面步骤目的是设置x-pack认证的用户名和密码（restFulAPI）
		CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "elastic"));
		restClientBuilder.setHttpClientConfigCallback((httpAsyncClientBuilder) -> {
			return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		});
		client = new RestHighLevelClient(restClientBuilder);
	}

	/**
	 * 删除索引
	 * 
	 * @param index
	 * @return
	 */
	public static boolean deleteWholeIndex(String index) {
		DeleteIndexRequest delrequest = new DeleteIndexRequest(index);
		try {
			DeleteIndexResponse deleteIndexResponse = client.indices().delete(delrequest, RequestOptions.DEFAULT);
			System.out.println("deleteWholeIndex execute: " + deleteIndexResponse.isAcknowledged());
			return true;
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return false;
	}

	/**
	 * 构造mapping
	 * 
	 * @param columns
	 * @return
	 */
	public static String makeTableMapping(String tabletype, String columns) {
		String[] columslist = columns.split(",");
		JSONObject tablejson = new JSONObject();
		JSONObject projson = new JSONObject();
		JSONObject fieldjson = new JSONObject();
		for (int i = 0; i < columslist.length; i++) {
			String[] fields = columslist[i].split(":");
			JSONObject fielddetailjson = new JSONObject();
			fielddetailjson.put("type", fields[1]);
			fieldjson.put(fields[0], fielddetailjson);
		}
		projson.put("properties", fieldjson);
		tablejson.put(tabletype, projson);
		return tablejson.toString();
	}

	public boolean checkRecordExists(String index, String tabletype, String id) {
		GetRequest getRequest = new GetRequest(index, tabletype, id);
		boolean exists;
		try {
			exists = client.exists(getRequest, RequestOptions.DEFAULT);
			return exists;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static boolean checkExists(String index) {
		GetIndexRequest getrequest = new GetIndexRequest();
		getrequest.indices(index);
		boolean exists;
		try {
			exists = client.indices().exists(getrequest, RequestOptions.DEFAULT);
			return exists;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 创建索引以及映射
	 * 
	 * @param index
	 * @return
	 */
//	@SuppressWarnings({ "unchecked" })
	public static boolean createIndex(String index) {

		GetIndexRequest getrequest = new GetIndexRequest();
		getrequest.indices(index);
		boolean exists = checkExists(index);
		if (true == exists) {
			deleteWholeIndex(index);
		}

		Yaml yaml = new Yaml();
//		URL url = ElasticSearchTools.class.getClassLoader().getResource("table_class_mapping.yaml");
//		URL es_config = ElasticSearchTools.class.getClassLoader().getResource("es_config.yaml");
		try {
//			Map<String, Object> table_map = yaml
//					.load(ElasticSearchTools.class.getClassLoader().getResourceAsStream("table_class_mapping.yaml"));
//			Map<String, Object> es_map = yaml.load(ElasticSearchTools.class.getClassLoader().getResourceAsStream("es_config.yaml"));
//			Map<String, String> table_detail = (Map<String, String>) table_map.get(index);
			CreateIndexRequest request = new CreateIndexRequest(index);
//			String columns = table_detail.get("column");
			String columns = "id:long,identity_id:text,fnumbe:long,earno:long,breeding:long,farm_id:long,estatus:text";
//			String requestjson = makeTableMapping((String) es_map.get("table_type"), columns);
			String requestjson = makeTableMapping("_doc", columns);
			request.settings(
					Settings.builder().put("index.number_of_shards", 5)
							.put("index.number_of_replicas", 1));
			request.mapping("_doc", requestjson.toString(), XContentType.JSON);
			CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
			System.out.println("execute: " + createIndexResponse.isAcknowledged());
			return true;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 加载json数据到es索引表，每次一条json数据
	 * 
	 * @param index
	 * @param source_text
	 * @return
	 */
	public boolean pulldata(String index, JSONObject source_text) {
		System.out.println("pulldata begin");
		IndexRequest request = new IndexRequest(index, "_doc");
		System.out.println("pulldata setid");
		request.id(Long.toString(source_text.getLong("id")));
		request.source(source_text.toString(), XContentType.JSON);
		try {
			System.out.println("begintpull...");
			IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
			System.out.println("pulldata execute: " + indexResponse.status());
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 加载json数据到es索引表，每次一条json数据
	 *
	 * @param index
	 * @return
	 */
	public boolean pulldata(String index, String str) {
		log.info("pulldata begin");
		IndexRequest request = new IndexRequest(index, "_doc");
		request.source(str, XContentType.JSON);
		try {
			IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
			log.info("es result:{1}", indexResponse.toString());
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void insertData(String indexName, String jsonString) throws IOException {
		System.out.println("ifcheck");
		boolean ifcheck = checkExists("bc_center_herd");
		System.out.println("ifcheck="+ifcheck);
		log.info("insert data");
		IndexRequest request = new IndexRequest(indexName,"doc");
		request.source(jsonString, XContentType.JSON);
		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
		log.info("es result:{1}" + indexResponse.toString());
	}

	/**
	 * 更新es索引数据，根据id每次更新一条记录
	 * 
	 * @param index
	 * @param type
	 * @param id
	 * @param jsonString
	 * @return
	 */
	public boolean updateRecord(String index, String type, String id, String jsonString) {
		UpdateRequest request = new UpdateRequest(index, type, id);
		request.doc(jsonString, XContentType.JSON);
		try {
			UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
			System.out.println("updateRecord execute: " + updateResponse.getGetResult());
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 删除一行索引数据，根据id删除
	 * 
	 * @param index
	 * @param type
	 * @param id
	 * @return
	 */
	public boolean deleteRecord(String index, String type, String id) {
		DeleteRequest request = new DeleteRequest(index, type, id);
		try {
			DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
			System.out.println("deleteResponse execute: " + deleteResponse.getResult());
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static void main(String[] args) throws Exception {
//		// es客户端连接
//		init_clinet();
//		Yaml yaml = new Yaml();
//		URL url = ElasticSearchTools.class.getClassLoader().getResource("table_class_mapping.yaml");
//		Map<String, Object> table_map = yaml.load(new FileInputStream(url.getFile()));
//
//		int argslength = args.length;
//		if (argslength == 1) {
//			int task_type = Integer.parseInt(args[0]);
//			switch (task_type) {
//			case 0:// 创建索引
//				for (String key : table_map.keySet()) {
//					createIndex(key);
//				}
//				break;
//			case 1:// 插入数据
//				JSONObject insertObject = JSONObject.parseObject(insertmessage);
//				for (String key : table_map.keySet()) {
//					Map<String, String> table_detail = (Map<String, String>) table_map.get(key);
//					if (insertObject.getString("type").equals("INSERT")) {
//						// 匹配表，有可能是分表，需要前缀匹配
//						if (insertObject.getString("table").equals(key) || insertObject.getString("table")
//								.startsWith(key + "_" + table_detail.get("database_fix") + "_")) {
//							// 匹配数据库,有可能分库，需要前缀匹配
//							if (insertObject.getString("database").equals(table_detail.get("database"))
//									|| insertObject.getString("database").startsWith(table_detail.get("database") + "_"
//											+ table_detail.get("database_fix") + "_")) {
//
//								String columns = table_detail.get("column");
//								JSONArray jsonlist = (JSONArray) insertObject.get("data");
//								String[] columslist = columns.split(",");
//								for (int i = 0; i < jsonlist.size(); i++) {
//									JSONObject one = new JSONObject();
//									for (int t = 0; t < columslist.length; t++) {
//										String[] fields = columslist[t].split(":");
//										switch (fields[1]) {
//										case "text":
//										case "string":
//										case "keyword":
//											one.put(fields[0], jsonlist.getJSONObject(i).getString(fields[0]));
//											break;
//										case "long":
//											one.put(fields[0], jsonlist.getJSONObject(i).getLong(fields[0]));
//											break;
//										case "date":
//											one.put(fields[0], jsonlist.getJSONObject(i).getDate(fields[0]));
//											break;
//										case "integer":
//											one.put(fields[0], jsonlist.getJSONObject(i).getInteger(fields[0]));
//											break;
//										case "double":
//											one.put(fields[0], jsonlist.getJSONObject(i).getDouble(fields[0]));
//											break;
//										case "boolean":
//											one.put(fields[0], jsonlist.getJSONObject(i).getBoolean(fields[0]));
//											break;
//										default:
//											one.put(fields[0], jsonlist.getJSONObject(i).getString(fields[0]));
//											break;
//										}
//									}
//									pulldata(key, one);
//								}
//							}
//						}
//					}
//				}
//				break;
//			case 2:// 更新数据
//				JSONObject updateObject = JSONObject.parseObject(updatemessage);
//				for (String key : table_map.keySet()) {
//					Map<String, String> table_detail = (Map<String, String>) table_map.get(key);
//					if (updateObject.getString("type").equals("UPDATE")) {
//						// 匹配表，有可能是分表，需要前缀匹配
//						if (updateObject.getString("table").equals(key)
//								|| updateObject.getString("table").startsWith(key + "_hiix_")) {
//
//							// 匹配数据库,有可能分库，需要前缀匹配
//							if (updateObject.getString("database").equals(table_detail.get("database"))
//									|| updateObject.getString("database").startsWith(table_detail.get("database") + "_"
//											+ table_detail.get("database_fix") + "_")) {
//								JSONArray updatecolumns = updateObject.getJSONArray("old");
//								JSONArray updatedata = updateObject.getJSONArray("data");
//
//								for (int i = 0; i < updatecolumns.size(); i++) {
//									JSONObject updateresult = new JSONObject();
//									JSONObject oneupdate = updatecolumns.getJSONObject(i);
//									for (String onekey : oneupdate.keySet()) {
//										updateresult.put(onekey, updatedata.getJSONObject(i).get(onekey));
//									}
//									updateRecord(key, "_doc", Long.toString(updatedata.getJSONObject(i).getLong("id")),
//											updateresult.toString());
//								}
//							}
//						}
//					}
//				}
//				break;
//			case 3:
//				JSONObject deleteObject = JSONObject.parseObject(deletemessage);
//				for (String key : table_map.keySet()) {
//					Map<String, String> table_detail = (Map<String, String>) table_map.get(key);
//					if (deleteObject.getString("type").equals("DELETE")) {
//						// 匹配表，有可能是分表，需要前缀匹配
//						if (deleteObject.getString("table").equals(key)
//								|| deleteObject.getString("table").startsWith(key + "_hiix_")) {
//
//							// 匹配数据库,有可能分库，需要前缀匹配
//							if (deleteObject.getString("database").equals(table_detail.get("database"))
//									|| deleteObject.getString("database").startsWith(table_detail.get("database") + "_"
//											+ table_detail.get("database_fix") + "_")) {
//								JSONArray deletedata = deleteObject.getJSONArray("data");
//								for (int i = 0; i < deletedata.size(); i++) {
//									JSONObject delresult = deletedata.getJSONObject(i);
//									deleteRecord(key, "_doc", Long.toString(delresult.getLong("id")));
//								}
//							}
//						}
//					}
//				}
//				break;
//			}
//		}
//
//		// 关闭es客户端连接
//		try {
//			client.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
