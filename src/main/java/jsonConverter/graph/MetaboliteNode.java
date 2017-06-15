package jsonConverter.graph;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class MetaboliteNode implements Node{

	private String name;
	private String id;
	private Compartment c;
	private List<String> lGraphs = new ArrayList<String>(2);
	
	public MetaboliteNode(String id, String name, Compartment compartment) {
		this.name = name;
		this.id = id;
		this.c = compartment;
	}
	
	public String getID() {
		// TODO Auto-generated method stub
		return name;
	}

	@SuppressWarnings("unchecked")
	public JSONObject getJSON_Object() {
		// TODO Auto-generated method stub
		JSONObject metabolite = new JSONObject();
		
		metabolite.put("id", id);
		
		JSONObject data = new JSONObject();
		data.put("name", name);
		metabolite.put("data", data);
		
		JSONObject meta = new JSONObject();
		meta.put("compartment", c.name().replaceAll("_", " "));
		metabolite.put("meta", meta);
		
		JSONArray graphsList = new JSONArray();
        
        for (String graph : lGraphs) {
			graphsList.add(graph);
		}
        
        metabolite.put("graphs", graphsList);
		
		return metabolite;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return getJSON_Object().toJSONString();
	}

	public void addGraph(String GraphID) {
		// TODO Auto-generated method stub
		lGraphs.add(GraphID);
	}
	
	
}