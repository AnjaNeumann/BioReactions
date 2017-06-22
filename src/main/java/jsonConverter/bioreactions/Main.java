package jsonConverter.bioreactions;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.json.JsonWriter;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import jsonConverter.graph.Edge;
import jsonConverter.graph.LogicGraph;
import jsonConverter.graph.MetaboliteNode;
import jsonConverter.graph.ReactionBlancNode;

public class Main {

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
		JSONParser jparser = new JSONParser();
		JSONObject job = (JSONObject)jparser.parse(new FileReader("Data/Rohdaten"));
//		LinkedList<Reaction> reactionlist = new LinkedList<Reaction>();
//		Map<String,Metabolite> metabolitemap = new HashMap<String,Metabolite>();
//		Map<String,Gene> genemap = new HashMap<String,Gene>();
//		List<String> subsystemlist = new LinkedList<String>();
//		Map<String,LinkedList<Reaction>> subsystems = new HashMap<String,LinkedList<Reaction>>();
//		String subsys;
		
//		JSONArray rea = (JSONArray) job.get("reactions");
//		for (int i= 0; i<rea.size(); i++){
//			
//			reactionlist.add(new Reaction((JSONObject)rea.get(i)));
//			
//			JSONObject jrea = (JSONObject)rea.get(i);
//			subsys = (String)jrea.get("subsystem");
//			if (!subsystemlist.contains(subsys) && subsys != null){
//				subsystemlist.add(subsys);
//				//System.out.println(subsys);
//			}
//			if (!subsystems.keySet().contains(subsys)){
//			
//				subsystems.put(subsys, new LinkedList<Reaction>());
//			}
//			subsystems.get(subsys).add(reactionlist.getLast());
//			
//		}
//		int mcnt = 0;
//		
//		for (String key : subsystems.keySet()){
//			LinkedList<Reaction> reas = subsystems.get(key);
//			List<String> metabolites = new LinkedList<String>();
//			for (Reaction current : reas){
//				for (String name : current.getMetabolites().keySet()){
//					if (!metabolites.contains(name))
//						metabolites.add(name);
//				}
//			}
//			
//			System.out.println(key +": "+ subsystems.get(key).size()+": "+metabolites.size());
//			mcnt +=metabolites.size();
//			
//		}
//		System.out.println("Metabolite:");
//		JSONArray metab = (JSONArray)job.get("number of all metabolites vs. number of metabolite kinds");
//		System.out.println(mcnt+ " vs "+metab.size());
//		
//		for (int i= 0; i<metab.size(); i++){
//			
//			metabolitemap.put((String)((JSONObject)metab.get(i)).get("id"),new Metabolite((JSONObject)metab.get(i)));
//			
//		
//		}
//		System.out.println("number of genes");
//		JSONArray genes = (JSONArray)job.get("genes");
//		System.out.println(genes.size());
//		
//		for (int i= 0; i<genes.size(); i++){
//			
//			genemap.put((String)((JSONObject)genes.get(i)).get("id"),new Gene((JSONObject)genes.get(i)));
//		
//		}
//		
//		FileWriter fw = new FileWriter("Data/Subsysteme");
//		fw.write("Subsystem\tReaction\tMetabolite\tValue\n");
//		for (String key : subsystems.keySet()){
//			LinkedList<Reaction> reactions = subsystems.get(key);
//			for (Reaction currentreaction : reactions){
//				HashMap<String, Integer> metabols = currentreaction.getMetabolites();
//				for (String m : metabols.keySet()){
//					fw.write(key+"\t"+currentreaction.getName()+"\t"+m+"\t"+metabols.get(m)+"\n");
//				}
//			}
//			
//		}
//		fw.close();
		
		//#################################################################################################
		
		//graphs.json
		//   ID      Name
		Map<String, LogicGraph> mLogicGraphs = new HashMap<String, LogicGraph>();
		 
		JSONObject jsonCompartments = (JSONObject) job.get("compartments");
		for (Object jsonCompartmentKey : jsonCompartments.keySet()) {
			String strKey = (String) jsonCompartmentKey;
			String strName = (String) jsonCompartments.get(jsonCompartmentKey);
			mLogicGraphs.put(strKey, new LogicGraph("compartment", strName, strKey));
		}
		
		//nodes.json + edges.json
		Map<String, MetaboliteNode> mMetabolites = new HashMap<String, MetaboliteNode>();
		
		JSONArray jsonMetabolites = (JSONArray) job.get("metabolites");
		for (Object jsonMetabolite  : jsonMetabolites) {
			String strKey = (String) ((JSONObject) jsonMetabolite).get("id");
			if (strKey.contains("_"))
			strKey = strKey.substring(0, strKey.lastIndexOf('_'));
			 mMetabolites.put(strKey, new MetaboliteNode(strKey, (String) ((JSONObject) jsonMetabolite).get("name")));
		}

		List<ReactionBlancNode> lReaktions = new ArrayList<ReactionBlancNode>();
		List<Edge>	lEdges = new ArrayList<Edge>();
		JSONArray jsonReactions = (JSONArray) job.get("reactions");
		for (Object jsonReaktionObject  : jsonReactions) {
			 JSONObject jsonReaction = (JSONObject) jsonReaktionObject;
			 ReactionBlancNode nodeReaction = new ReactionBlancNode((String) jsonReaction.get("name"));
			 lReaktions.add(nodeReaction);
			 
			 LogicGraph graphReaction = new LogicGraph("reaction", (String) jsonReaction.get("name"), null);
			 mLogicGraphs.put((String) jsonReaction.get("name"), graphReaction);
			 String strReactionGraphUUID = graphReaction.getStrUUID();
			 
			 String strsubsystemGraphUUID = null;
			 String subsystem = (String) jsonReaction.get("subsystem");
			 if (subsystem != null)
			 {
				 if (!mLogicGraphs.containsKey(subsystem))
				 {
					 mLogicGraphs.put(subsystem, new LogicGraph("subsystem", subsystem, null));
				 }
				 
				 strsubsystemGraphUUID = mLogicGraphs.get(subsystem).getStrUUID();
			 }
			 if (strsubsystemGraphUUID != null) nodeReaction.addGraph(strsubsystemGraphUUID);
			 nodeReaction.addGraph(strReactionGraphUUID);
			 
				 
			 JSONObject metabolite = (JSONObject) jsonReaction.get("metabolites");
			 
			 for (Object metaboliteName : metabolite.keySet()) {
				String strMetaboliteName = (String) metaboliteName;
				String strCompartmentID = null;
				if (strMetaboliteName.contains("_"))
				{
					strCompartmentID = mLogicGraphs.get(strMetaboliteName.substring(strMetaboliteName.lastIndexOf('_')+1)).getStrUUID();
					strMetaboliteName = strMetaboliteName.substring(0, strMetaboliteName.lastIndexOf('_'));
					mMetabolites.get(strMetaboliteName).addGraph(strCompartmentID);;
					nodeReaction.addGraph(strCompartmentID);
				}
				
				
				 Double fCoefficient = (Double) metabolite.get(metaboliteName);

				 if (strsubsystemGraphUUID != null)
				 mMetabolites.get(strMetaboliteName).addGraph(strsubsystemGraphUUID);
				 mMetabolites.get(strMetaboliteName).addGraph(strReactionGraphUUID);
				 String MetaboliteUUID = mMetabolites.get(strMetaboliteName).getStrUUID();
				 
				 Edge currentEdge = null;
				 if (fCoefficient < 0)
				 {
					 currentEdge = new Edge(MetaboliteUUID, nodeReaction.getStrUUID(), fCoefficient);
				 } else {
					 currentEdge = new Edge(nodeReaction.getStrUUID(), MetaboliteUUID, fCoefficient);
				 }
				 
				 if (strCompartmentID != null)
					 currentEdge.addGraph(strCompartmentID);
				 
				 if (strsubsystemGraphUUID != null) currentEdge.addGraph(strsubsystemGraphUUID);
				 currentEdge.addGraph(strReactionGraphUUID);
				 lEdges.add(currentEdge);
			 }
		}
		
		FileWriter filewriter = new FileWriter("edges.json");
		
		for (Edge edge : lEdges) {
			filewriter.write(edge.toJSONString() + '\n');
		}
		filewriter.close();
		
		filewriter = new FileWriter("graphs.json");
		for (LogicGraph graph : mLogicGraphs.values()) {
			filewriter.write(graph.toJSONString() + '\n');
		}
		filewriter.close();
		
		filewriter = new FileWriter("nodes.json");
		for (ReactionBlancNode reactionBlancNode : lReaktions) {
			filewriter.write(reactionBlancNode.toJSONString() + '\n');
		}
		
		for (MetaboliteNode metabolite : mMetabolites.values()) {
			filewriter.write(metabolite.toJSONString() + '\n');
		}
		
	}

}
