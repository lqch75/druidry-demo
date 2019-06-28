package com.gwhome.bigdata.po;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DruidResponse {

	private String timestamp;

	private Object result;

	@Override
	public String toString() {
		return "{" + "timestamp=" + timestamp + ", result='" + result + '}';
	}
}
