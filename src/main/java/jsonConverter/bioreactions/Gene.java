package jsonConverter.bioreactions;

import java.util.ArrayList;

import org.json.simple.JSONObject;

public class Gene {
	private String name;
	private ArrayList<String> biggIds;
	private String id;
	
	@SuppressWarnings("unchecked")
	public Gene(JSONObject gene){
		this.name = (String)gene.get("name");
		this.id = (String)gene.get("id");
		JSONObject notes = (JSONObject)gene.get("notes");
		this.biggIds = (ArrayList<String>) notes.get("original_bigg_ids");
	}

	public String getName() {
		return name;
	}

	public ArrayList<String> getBiggIds() {
		return biggIds;
	}

	public String getId() {
		return id;
	}

}
