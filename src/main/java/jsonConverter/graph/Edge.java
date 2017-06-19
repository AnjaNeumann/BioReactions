package jsonConverter.graph; 

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Edge extends JSONObject{
	

	JSONArray m_LogicGraphsList = new JSONArray();

	@SuppressWarnings("unchecked")
	public Edge(String source, String target, String id, Compartment compartment, Integer quantity) {
		super();
		this.put("id", id);
        this.put("target", target);
        this.put("source", source);
        	        
        JSONObject dataObj = new JSONObject(); 
        if (quantity.equals(0)) dataObj.put("quantity", quantity);
        this.put("data", dataObj);
        
        JSONObject metaObj = new JSONObject(); 
        this.put("meta", metaObj);
        
        this.put("graphs", m_LogicGraphsList);
        
	}
	@SuppressWarnings("unchecked")
	public void addGraph(String GraphID) {
		m_LogicGraphsList.add(GraphID);
	}
	
	
}