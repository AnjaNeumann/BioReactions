package jsonConverter.graph;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;

public class ReactionNode implements Node{

	private String id;
	private List<String> lGraphs = new ArrayList<String>(2);
	
	public ReactionNode(String id) {
		this.id = id;
	}
	
	public String getID() {
		return id;
	}

	public JSONObject getJSON_Object() {
		// TODO Auto-generated method stub
		JSONObject reaktion = new JSONObject();
		return reaktion;
	}

	public void addGraph(String GraphID) {
		// TODO Auto-generated method stub
		lGraphs.add(GraphID);
	}
	
}