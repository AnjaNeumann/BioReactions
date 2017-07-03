package jsonConverter.graph;

import java.util.UUID;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@SuppressWarnings("serial")
public class MetaboliteNode extends JSONObject{
	
	
	private JSONArray m_LogicGraphsList = new JSONArray();
	final private String strUUID = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 24);
	
	
	@SuppressWarnings("unchecked")
	public MetaboliteNode(String id, String name) {
		super();
		this.put("id", strUUID);
		
		JSONObject data = new JSONObject();
		data.put("name", name);
		data.put("oldID", id);
		this.put("data", data);
		
		JSONObject meta = new JSONObject();
		this.put("meta", meta);
		
		this.put("graphs", m_LogicGraphsList);
	}

	@SuppressWarnings("unchecked")
	public void addGraph(String GraphID) {
		if (!m_LogicGraphsList.contains(GraphID))
		m_LogicGraphsList.add(GraphID);
	}
	
	@SuppressWarnings("unchecked")
	public void setQuantityOfCompartment(String compartmentID, Integer quantity)
	{
		this.put(compartmentID, quantity);
	}

	public String getStrUUID() {
		return strUUID;
	}
}