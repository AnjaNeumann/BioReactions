package jsonConverter.bioreactions;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Main {

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
		JSONParser jparser = new JSONParser();
		JSONObject job = (JSONObject)jparser.parse(new FileReader("Data/Rohdaten"));
		LinkedList<Reaction> reactionlist = new LinkedList<Reaction>();
		Map<String,Metabolite> metabolitemap = new HashMap<String,Metabolite>();
		List<String> subsystemlist = new LinkedList<String>();
		Map<String,LinkedList<Reaction>> subsystems = new HashMap<String,LinkedList<Reaction>>();
		String subsys;
		
		JSONArray rea = (JSONArray) job.get("reactions");
		for (int i= 0; i<rea.size(); i++){
			
			reactionlist.add(new Reaction((JSONObject)rea.get(i)));
			
			JSONObject jrea = (JSONObject)rea.get(i);
			subsys = (String)jrea.get("subsystem");
			if (!subsystemlist.contains(subsys) && subsys != null){
				subsystemlist.add(subsys);
				//System.out.println(subsys);
			}
			if (!subsystems.keySet().contains(subsys)){
			
				subsystems.put(subsys, new LinkedList<Reaction>());
			}
			subsystems.get(subsys).add(reactionlist.getLast());
			
		}
		int mcnt = 0;
		
		for (String key : subsystems.keySet()){
			LinkedList<Reaction> reas = subsystems.get(key);
			List<String> metabolites = new LinkedList();
			for (Reaction current : reas){
				for (String name : current.getMetabolites().keySet()){
					if (!metabolites.contains(name))
						metabolites.add(name);
				}
			}
			
			System.out.println(key +": "+ subsystems.get(key).size()+": "+metabolites.size());
			mcnt +=metabolites.size();
			
		}		
		JSONArray metab = (JSONArray)job.get("metabolites");
		System.out.println(mcnt+ " vs "+metab.size());
		
		for (int i= 0; i<metab.size(); i++){
			
			metabolitemap.put((String)((JSONObject)metab.get(i)).get("id"),new Metabolite((JSONObject)metab.get(i)));
			
		
		}
		FileWriter fw = new FileWriter("Data/Subsysteme");
		fw.write("Subsystem\tReaction\tMetabolite\tValue\n");
		for (String key : subsystems.keySet()){
			LinkedList<Reaction> reactions = subsystems.get(key);
			for (Reaction currentreaction : reactions){
				HashMap<String, Integer> metabols = currentreaction.getMetabolites();
				for (String m : metabols.keySet()){
					fw.write(key+"\t"+currentreaction.getName()+"\t"+m+"\t"+metabols.get(m)+"\n");
				}
			}
			
		}
		fw.close();
		
		
		
		

	}

}
