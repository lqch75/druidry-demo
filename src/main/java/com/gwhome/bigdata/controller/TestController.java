package com.gwhome.bigdata.controller;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gwhome.bigdata.po.DruidResponse;

import in.zapr.druid.druidry.Interval;
import in.zapr.druid.druidry.aggregator.CountAggregator;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.client.DruidClient;
import in.zapr.druid.druidry.client.DruidConfiguration;
import in.zapr.druid.druidry.client.DruidJerseyClient;
import in.zapr.druid.druidry.client.exception.ConnectionException;
import in.zapr.druid.druidry.dimension.DruidDimension;
import in.zapr.druid.druidry.dimension.SimpleDimension;
import in.zapr.druid.druidry.filter.AndFilter;
import in.zapr.druid.druidry.filter.SelectorFilter;
import in.zapr.druid.druidry.granularity.Granularity;
import in.zapr.druid.druidry.granularity.PredefinedGranularity;
import in.zapr.druid.druidry.granularity.SimpleGranularity;
import in.zapr.druid.druidry.query.aggregation.DruidTimeSeriesQuery;
import in.zapr.druid.druidry.query.aggregation.DruidTopNQuery;
import in.zapr.druid.druidry.query.select.DruidSelectQuery;
import in.zapr.druid.druidry.query.select.PagingSpec;
import in.zapr.druid.druidry.topNMetric.SimpleMetric;
import in.zapr.druid.druidry.topNMetric.TopNMetric;

@RestController
public class TestController {

	private Logger log = LoggerFactory.getLogger(this.getClass());

	@RequestMapping(value = "/timeseries")
	public Object Timeseries() throws JsonProcessingException {

		/**
		 * 1、Timeseries
		 * 
		 * 对于需要统计一段时间内的汇总数据，或者是指定时间粒度的汇总数据，druid可以通过Timeseries来完成。
		 * 
		 * select count(*) from sqltest where channel = 'n001' and cityName = 'y'
		 */

		SelectorFilter selectorFilter1 = new SelectorFilter("channel", "n001");
		SelectorFilter selectorFilter2 = new SelectorFilter("cityName", "y");

		AndFilter filter = new AndFilter(Arrays.asList(selectorFilter1, selectorFilter2));

		DruidAggregator aggregator1 = new CountAggregator("count");

		DateTime startTime = new DateTime(2015, 9, 11, 0, 0, 0, DateTimeZone.forID("Asia/Shanghai"));
		DateTime endTime = new DateTime(2015, 9, 13, 0, 0, 0, DateTimeZone.forID("Asia/Shanghai"));
		Interval interval = new Interval(startTime, endTime);

		Granularity granularity = new SimpleGranularity(PredefinedGranularity.MINUTE);

		DruidTimeSeriesQuery query = DruidTimeSeriesQuery.builder().dataSource("sqltest").descending(true)
				.granularity(granularity).filter(filter).aggregators(Arrays.asList(aggregator1))
				.intervals(Collections.singletonList(interval)).build();

		ObjectMapper mapper = new ObjectMapper();
		String requiredJson = mapper.writeValueAsString(query);
		log.info("requiredJson:{}", requiredJson);

		DruidConfiguration config = DruidConfiguration.builder().host("192.168.240.151").port(8082)
				.endpoint("druid/v2/").build();

		DruidClient client = new DruidJerseyClient(config);
		try {
			client.connect();
			List<DruidResponse> responses = client.query(query, DruidResponse.class);

			for (DruidResponse timeseriesResponse : responses) {
				log.info("timeseriesResponse:{}", timeseriesResponse.toString());
			}
			client.close();
			return responses;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (ConnectionException e) {
				e.printStackTrace();
			}
		}

		return "Timeseries ! ";
	}

	@RequestMapping(value = "/topn")
	public Object TopN() throws JsonProcessingException {

		/**
		 * TopN
		 * 
		 * 返回指定维度和排序字段的有序top-n序列.TopN支持返回前N条记录，并支持指定的Metric为排序依据
		 */

		DruidAggregator aggregator1 = new CountAggregator("count");

		DateTime startTime = new DateTime(2015, 9, 11, 0, 0, 0, DateTimeZone.forID("Asia/Shanghai"));
		DateTime endTime = new DateTime(2015, 9, 13, 0, 0, 0, DateTimeZone.forID("Asia/Shanghai"));
		Interval interval = new Interval(startTime, endTime);

		Granularity granularity = new SimpleGranularity(PredefinedGranularity.MINUTE);

		DruidDimension dimension = new SimpleDimension("channel");
		TopNMetric metric = new SimpleMetric("count");

		DruidTopNQuery query = DruidTopNQuery.builder().dataSource("sqltest").dimension(dimension).threshold(2)
				.topNMetric(metric).granularity(granularity).aggregators(Arrays.asList(aggregator1))
				.intervals(Collections.singletonList(interval)).build();

		ObjectMapper mapper = new ObjectMapper();
		String requiredJson = mapper.writeValueAsString(query);
		log.info("requiredJson:{}", requiredJson);

		DruidConfiguration config = DruidConfiguration.builder().host("192.168.240.151").port(8082)
				.endpoint("druid/v2/").build();

		DruidClient client = new DruidJerseyClient(config);
		try {
			client.connect();
			List<DruidResponse> responses = client.query(query, DruidResponse.class);

			for (DruidResponse timeseriesResponse : responses) {
				log.info("timeseriesResponse:{}", timeseriesResponse.toString());
			}

			client.close();
			return responses;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (ConnectionException e) {
				e.printStackTrace();
			}
		}

		return "TopN ! ";
	}

	@RequestMapping(value = "/select")
	public Object Select() throws JsonProcessingException {

		/**
		 * Select
		 * 
		 * select 类似于sql中select操作，select用来查看druid中的存储的数据，并支持按照指定过滤器和时间段查看指定维度和metric
		 * 能通过descending字段指定排序顺序，并支持分页拉取，但不支持aggregations和postAggregations
		 * 
		 * select channel, modifytime, cityName from sqltest limit 5
		 */

		DateTime startTime = new DateTime(2015, 9, 11, 0, 0, 0, DateTimeZone.forID("Asia/Shanghai"));
		DateTime endTime = new DateTime(2015, 9, 13, 0, 0, 0, DateTimeZone.forID("Asia/Shanghai"));
		Interval interval = new Interval(startTime, endTime);

		Granularity granularity = new SimpleGranularity(PredefinedGranularity.MINUTE);

		DruidDimension dimension1 = new SimpleDimension("channel");
		DruidDimension dimension2 = new SimpleDimension("modifytime");
		DruidDimension dimension3 = new SimpleDimension("cityName");

		PagingSpec pagingSpec = new PagingSpec(5, new HashMap<String, Integer>());

		DruidSelectQuery query = DruidSelectQuery.builder().dataSource("sqltest")
				.dimensions(Arrays.asList(dimension1, dimension2, dimension3)).granularity(granularity)
				.pagingSpec(pagingSpec).intervals(Collections.singletonList(interval)).build();

		ObjectMapper mapper = new ObjectMapper();
		String requiredJson = mapper.writeValueAsString(query);
		log.info("requiredJson:{}", requiredJson);

		DruidConfiguration config = DruidConfiguration.builder().host("192.168.240.151").port(8082)
				.endpoint("druid/v2/").build();

		DruidClient client = new DruidJerseyClient(config);
		try {
			client.connect();
			List<DruidResponse> responses = client.query(query, DruidResponse.class);

			for (DruidResponse timeseriesResponse : responses) {
				log.info("timeseriesResponse:{}", timeseriesResponse.toString());
			}

			client.close();
			return responses;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (ConnectionException e) {
				e.printStackTrace();
			}
		}

		return "Select ! ";
	}
}
