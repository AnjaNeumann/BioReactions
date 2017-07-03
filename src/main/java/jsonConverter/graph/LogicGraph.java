package jsonConverter.graph;

import java.util.UUID;

import org.json.simple.JSONObject;

@SuppressWarnings("serial")
public class LogicGraph extends JSONObject{

	final private String strUUID = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 24);
	
	@SuppressWarnings("unchecked")
	public LogicGraph(String strLable, String strName, String strOldID) {
		super();
		this.put("id", strUUID);
		
		JSONObject data = new JSONObject();
		data.put("name", strName);
		
		if (strOldID != null)
		data.put("id", strOldID);
		
		this.put("data", data);
		
		JSONObject meta = new JSONObject();
		meta.put("lable", strLable);
		this.put("meta", meta);
}

	
	public String getStrUUID() {
		return strUUID;
	}
	
	
}
