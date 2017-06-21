package jsonConverter.graph; 

import java.util.UUID;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Edge extends JSONObject{
	

	JSONArray m_LogicGraphsList = new JSONArray();
	final private String strUUID = UUID.randomUUID().toString();
	
	@SuppressWarnings("unchecked")
	public Edge(String source, String target, Double quantity) {
		super();
		this.put("id", strUUID);
        this.put("target", target);
        this.put("source", source);
        	        
        JSONObject dataObj = new JSONObject(); 
        if (quantity != null) dataObj.put("quantity", quantity);
        this.put("data", dataObj);
        
        JSONObject metaObj = new JSONObject(); 
        this.put("meta", metaObj);
        
        this.put("graphs", m_LogicGraphsList);
        
	}
	@SuppressWarnings("unchecked")
	public void addGraph(String GraphID) {
		m_LogicGraphsList.add(GraphID);
	}
	public String getStrUUID() {
		return strUUID;
	}
	
	
}