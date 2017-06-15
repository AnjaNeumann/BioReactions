package jsonConverter.bioreactions;

import java.util.ArrayList;

import org.json.simple.JSONObject;

public class Metabolite {
	private String name;
	private ArrayList<String> biggIds;
	private String id;
	private String compartment;
	
	@SuppressWarnings("unchecked")
	public Metabolite(JSONObject metabolite){
		this.name = (String)metabolite.get("name");
		this.id = (String)metabolite.get("id");
		this.compartment = (String)metabolite.get("compartment");
		JSONObject notes = (JSONObject)metabolite.get("notes");
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

	public String getCompartment() {
		return compartment;
	}
	

}
