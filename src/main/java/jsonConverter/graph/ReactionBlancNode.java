package jsonConverter.graph;

import org.gradoop.common.model.impl.id.GradoopId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@SuppressWarnings("serial")
public class ReactionBlancNode extends JSONObject{

	JSONArray m_LogicGraphsList = new JSONArray();
	final private String strUUID = GradoopId.get().toString();
	
	@SuppressWarnings("unchecked")
	public ReactionBlancNode(String label) {
		super();
		this.put("id", strUUID);
		
		JSONObject data = new JSONObject();
		data.put("type", "reaction_blank");
		this.put("data", data);
		
		JSONObject meta = new JSONObject();
		meta.put("label", label);
		meta.put("graphs", m_LogicGraphsList);
		this.put("meta", meta);
	}
	
	@SuppressWarnings("unchecked")
	public void addGraph(String GraphID) {
		if (!m_LogicGraphsList.contains(GraphID))
		m_LogicGraphsList.add(GraphID);
	}

	public String getStrUUID() {
		return strUUID;
	}
	
}