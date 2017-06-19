package jsonConverter.graph;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@SuppressWarnings("serial")
public class MetaboliteNode extends JSONObject{
	
	
	JSONArray m_LogicGraphsList = new JSONArray();
	
	@SuppressWarnings("unchecked")
	public MetaboliteNode(String id, String name) {
		super();
		this.put("id", id);
		
		JSONObject data = new JSONObject();
		data.put("name", name);
		this.put("data", data);
		
		JSONObject meta = new JSONObject();
		this.put("meta", meta);
		
		this.put("graphs", m_LogicGraphsList);
	}

	@SuppressWarnings("unchecked")
	public void addGraph(String GraphID) {
		m_LogicGraphsList.add(GraphID);
	}
	
	@SuppressWarnings("unchecked")
	public void setQuantityOfCompartment(Compartment compartment, Integer quantity)
	{
		this.put(compartment.getID(), quantity);
	}
}