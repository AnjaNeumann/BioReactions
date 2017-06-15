package jsonConverter.bioreactions;

import java.util.ArrayList;
import java.util.HashMap;

import org.json.simple.JSONObject;

public class Reaction {
	private String name;
	private HashMap<String,Integer> metabolites;
	private HashMap<String,String> annotations;
	private ArrayList<String> biggIds;
	private String id;
	private String subsystem;
	private String genereactionrule;
	
	@SuppressWarnings("unchecked")
	public Reaction(JSONObject rea){
		this.name = (String)rea.get("name");
		this.id = (String)rea.get("id");
		this.genereactionrule = (String)rea.get("genereactionrule");
		this.subsystem = (String)rea.get("subsystem");
		this.metabolites = (HashMap<String,Integer>)rea.get("metabolites");
		this.annotations = (HashMap<String,String>)rea.get("annotations");
		
		JSONObject notes = (JSONObject)rea.get("notes");
		this.biggIds = (ArrayList<String>) notes.get("original_bigg_ids");
		
	}
	
	public HashMap<String, String> getAnnotations() {
		return annotations;
	}

	
	public String getName() {
		return name;
	}
	public HashMap<String, Integer> getMetabolites() {
		return metabolites;
	}
	public ArrayList <String> getBiggIds() {
		return biggIds;
	}
	public String getId() {
		return id;
	}
	public String getSubsystem() {
		return subsystem;
	}
	public String getGenereactionrule() {
		return genereactionrule;
	}
}
