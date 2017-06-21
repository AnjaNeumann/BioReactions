package jsonConverter.graph;

import java.util.UUID;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@SuppressWarnings("serial")
public class ReactionBlancNode extends JSONObject{

	JSONArray m_LogicGraphsList = new JSONArray();
	final private String strUUID = UUID.randomUUID().toString();
	
	@SuppressWarnings("unchecked")
	public ReactionBlancNode(String name) {
		super();
		this.put("id", strUUID);
		
		JSONObject data = new JSONObject();
		data.put("name", name);
		this.put("data", data);
		
		this.put("graphs", m_LogicGraphsList);
	}
	
	@SuppressWarnings("unchecked")
	public void addGraph(String GraphID) {
		// TODO Auto-generated method stub
		m_LogicGraphsList.add(GraphID);
	}

	public String getStrUUID() {
		return strUUID;
	}
	
}