package jsonConverter.graph; 

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Edge {
	private Node source;
	private Node target;
	private String id;
	private Integer quantity;
	private List<String> lGraphs = new ArrayList<String>(2);

	public Edge(Node source, Node target, String id, Integer quantity) {
		// TODO Auto-generated constructor stub
		this.source = source;
		this.target = target;
		this.id = id;
		this.quantity = quantity;
	}
	
	public Edge(Node source, Node target, String id) {
		// TODO Auto-generated constructor stub
		this.source = source;
		this.target = target;
		this.id = id;
		this.quantity = 0;
	}
	
	public void addGraph(String GraphID)
	{
		lGraphs.add(GraphID);
	}
	
	@SuppressWarnings("unchecked")
	public JSONObject toJSON_Object() {
		// TODO Auto-generated method stub
		 JSONObject edge = new JSONObject();
	        edge.put("id", this.id);
	        edge.put("target", this.target.getID());
	        edge.put("source", this.source.getID());
	        	        
	        JSONObject dataObj = new JSONObject(); 
	        if (quantity.equals(0)) dataObj.put("quantity", quantity);
	        edge.put("data", dataObj);
	        
	        JSONObject metaObj = new JSONObject(); 
	        edge.put("meta", metaObj);
	        
	        JSONArray graphsList = new JSONArray();
	        
	        for (String graph : lGraphs) {
				graphsList.add(graph);
			}
	        
	        metaObj.put("graphs", graphsList);
	        
	        System.out.println(edge.toString());
	        return edge;
	}	
	
}