package jsonConverter.graph;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ReactionNode extends JSONObject{

	JSONArray m_LogicGraphsList = new JSONArray();
	
	@SuppressWarnings("unchecked")
	public ReactionNode(String id) {
		super();
		this.put("id", id);
		this.put("graphs", m_LogicGraphsList);
	}
	
	@SuppressWarnings("unchecked")
	public void addGraph(String GraphID) {
		// TODO Auto-generated method stub
		m_LogicGraphsList.add(GraphID);
	}
	
}