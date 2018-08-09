package com.lyq.es.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class DemoApplication {

	@Autowired
	private TransportClient client;


	@GetMapping("/people/man")
	@ResponseBody
	public String getMan(@RequestParam(name="id",defaultValue = "") String id){
		GetResponse result = client.prepareGet("people", "man", id).get();
		return result.toString();
	}

	@PostMapping("/people/man")
	@ResponseBody
	public String addMan(@RequestParam("name") String name,@RequestParam("age") Integer age) throws IOException {

		XContentBuilder content = XContentFactory.jsonBuilder()
			.startObject()
			.field("name", name)
			.field("age", age)
			.endObject();

		IndexResponse result = client.prepareIndex("people", "man").setSource(content).get();
		return result.toString();
	}

	@DeleteMapping("/people/man")
	@ResponseBody
	public String deleteMan(@RequestParam("id") String id){
		DeleteResponse result = client.prepareDelete("people", "man", id).get();
		return result.toString();
	}

	@PutMapping("/people/man")
	@ResponseBody
	public String updateMan(@RequestParam("id") String id,
		@RequestParam("name") String name,
		@RequestParam("age") Integer age) throws IOException, ExecutionException, InterruptedException {
		UpdateRequest updateRequest = new UpdateRequest("people","man",id);

		XContentBuilder content = XContentFactory.jsonBuilder()
			.startObject()
			.field("name", name)
			.field("age", age)
			.endObject();

		 updateRequest.doc(content);

		UpdateResponse result = client.update(updateRequest).get();

		return result.toString();
	}

	@PostMapping("/query")
	@ResponseBody
	public String query(@RequestParam("name") String name,@RequestParam("gteAge") Integer gteAge){

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.must(QueryBuilders.matchQuery("name",name));

		RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age")
			.from(gteAge);

		boolQueryBuilder.filter(rangeQueryBuilder);

		SearchResponse searchResponse = client.prepareSearch("people")
			.setTypes("man")
			.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			.setQuery(boolQueryBuilder)
			.setFrom(0)
			.setSize(10)
			.get();

		List<Map<String,Object>> result = new ArrayList<>();
		for (SearchHit hit : searchResponse.getHits()) {
			result.add(hit.getSourceAsMap());
		}

		return result.toString();
	}

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}
}
