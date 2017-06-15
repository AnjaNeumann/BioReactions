package jsonConverter.graph;

import org.json.simple.JSONObject;

public interface Node {
	String getID();
	JSONObject getJSON_Object();
	void addGraph(String GraphID); 
}