package com.example.controller;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.admin.KafkaAdminUtil;

@RestController
@RequestMapping("/api")
public class AdminController {
	
	@Autowired
	KafkaAdminUtil kafkaAdminUtil;	
	
	
	@GetMapping(value = "/createTopic/{name}")
	public String creatTopic(@PathVariable("name") String name) throws ExecutionException, InterruptedException {
		kafkaAdminUtil.createTopic(name, 1, (short)1);
		return "Success";		
	}
	
	@GetMapping(value= "/listTopics")
	public String listTopics() throws ExecutionException, InterruptedException{
		kafkaAdminUtil.listTopics();
		return "Success";
	}
	
	@GetMapping(value= "/describeTopics/{name}")
	public String describeTopic(@PathVariable("name") String name) throws ExecutionException, InterruptedException{
		kafkaAdminUtil.describeTopics(name);
		return "Success";
	}
	
	@GetMapping(value= "/deleteTopic/{name}")
	public String deleteTopic(@PathVariable("name") String name) throws ExecutionException, InterruptedException{
		kafkaAdminUtil.deleteTopic(name);
		return "Success";
	}
}
