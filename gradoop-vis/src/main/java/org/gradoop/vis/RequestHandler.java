/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.vis;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.preprocess.AvailablePreprocessings;
import org.gradoop.flink.model.impl.operators.preprocess.NeighborhoodGraph;
import org.gradoop.flink.model.impl.operators.preprocess.Pair;
import org.gradoop.vis.functions.*;
import org.gradoop.vis.pojo.GroupingRequest;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.MaxAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.MinAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.SumAggregator;
import org.gradoop.flink.util.GradoopFlinkConfig;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Handles REST requests to the server.
 */
@Path("")
public class RequestHandler {

    private final String META_FILENAME = "/metadata.json";

    private static final ExecutionEnvironment ENV = ExecutionEnvironment.createLocalEnvironment();
    private GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(ENV);

    AvailablePreprocessings samplings = new AvailablePreprocessings();

    /**
     * Creates a list of all available databases from the file structure under the /data/ folder.
     *
     * @return List of folders (datbases) under the /data/ folder.
     */
    @GET
    @Path("/databases")
    @Produces("application/json;charset=utf-8")
    public Response getDatabases() {
        JSONObject jsonObject = new JSONObject();

        // get all subfolders of "/data/", they are considered as databases
        File dataFolder = new File(RequestHandler.class.getResource("/data/").getFile());
        String[] databases = dataFolder.list((current, name) -> new File(current, name).isDirectory());
        for(String s : databases) {
            JSONArray jsonArray1 = new JSONArray();
            File dataFolder2 = new File(RequestHandler.class.getResource("/data/"+s+"/").getFile());
            String[] databases2 = dataFolder2.list((current, name) -> new File(current, name).isDirectory());
            for(String ss : databases2) {
                jsonArray1.put(ss);
            }
            try {
                jsonObject.put(s,jsonArray1);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

//
//
//        File dataFolder = new File(RequestHandler.class.getResource("/data/").getFile());
//        String[] databases = dataFolder.list((current, name) -> new File(current, name).isDirectory());

        // return the found databases to the client
//        assert databases != null;
//        for (String database : databases) {
//            jsonArray.put(database);
//        }
        return Response.ok(jsonObject.toString()).header("Access-Control-Allow-Origin", "*").build();
    }

    /**
     * Creates a list of all available sampling methods.
     *
     * @return a list of all available sampling methods.
     */
    @GET
    @Path("/samplings")
    @Produces("application/json;charset=utf-8")
    public Response getSamplings() {
        return Response.ok(samplings.getAvailablePreprocessings().toString())
                .header("Access-Control-Allow-Origin", "*").build();
    }

    /**
     * Takes a database name via a POST request and returns the keys of all
     * vertex and edge properties, and a boolean value specifying if the property has a numerical
     * type. The return is a string in the JSON format, for easy usage in a JavaScript web page.
     *
     * @param databaseName name of the loaded database
     * @return  A JSON containing the vertices and edges property keys
     */
    @POST
    @Path("/keys/{databaseName}")
    @Produces("application/json;charset=utf-8")
    public Response getKeysAndLabels(@PathParam("databaseName") String databaseName) {
        databaseName = databaseName.replace("-","/");
        URL meta = RequestHandler.class.getResource("/data/" + databaseName + META_FILENAME);
        String path = RequestHandler.class.getResource("/data/" + databaseName).getPath();
        try {
            if (meta == null) {
                JSONObject result = computeKeysAndLabels(path);
                if (result == null) {
                    return Response.serverError().build();
                }
                return Response.ok(result.toString())
                        .header("Access-Control-Allow-Origin", "*")
                        .build();
            } else {
                JSONObject result = readKeysAndLabels(databaseName);
                if (result == null) {
                    return Response.serverError().build();
                }
                return Response.ok(readKeysAndLabels(databaseName).toString())
                        .header("Access-Control-Allow-Origin", "*")
                        .build();
            }
        } catch (Exception e) {
            e.printStackTrace();
            // if any exception is thrown, return an error to the client
            return Response.serverError().build();
        }
    }

    /**
     * Compute property keys and labels.
     * @param path path of the database
     * @return JSONObject containing property keys and labels
     */
    private JSONObject computeKeysAndLabels(String path) throws IOException {
        LogicalGraph graph = readLogicalGraph(path);
        JSONObject jsonObject = new JSONObject();

        //compute the vertex and edge property keys and return them
        try {
            jsonObject.put("vertexKeys", getVertexKeys(graph));
            jsonObject.put("edgeKeys", getEdgeKeys(graph));
            jsonObject.put("vertexLabels", getVertexLabels(graph));
            jsonObject.put("edgeLabels", getEdgeLabels(graph));
            FileWriter writer = new FileWriter(path + META_FILENAME);
            jsonObject.write(writer);
            writer.flush();
            writer.close();

            return jsonObject;
        } catch (Exception e) {
            e.printStackTrace();
            // if any exception is thrown, return an error to the client
            return null;
        }
    }

    /**
     * Read the property keys and labels from the buffered JSON.
     * @param databaseName name of the database
     * @return JSONObject containing the property keys and labels
     * @throws IOException if reading fails
     * @throws JSONException if JSON creation fails
     */
    private JSONObject readKeysAndLabels(String databaseName) throws IOException, JSONException {
        String dataPath = RequestHandler.class.getResource("/data/" + databaseName).getFile();
        String content =
                new String(Files.readAllBytes(Paths.get(dataPath + META_FILENAME)), StandardCharsets.UTF_8);

        return new JSONObject(content);
    }

    /**
     * Takes any given graph and creates a JSONArray containing the vertex property keys and a
     * boolean,
     * specifying it the property has a numerical type.
     *
     * @param graph input graph
     * @return  JSON array with property keys and boolean, that is true if the property type is
     * numercial
     * @throws Exception if the collecting of the distributed data fails
     */
    private JSONArray getVertexKeys(LogicalGraph graph) throws Exception {

        List<Tuple3<Set<String>, String, Boolean>> vertexKeys = graph.getVertices()
                .flatMap(new PropertyKeyMapper<>())
                .groupBy(1)
                .reduceGroup(new LabelGroupReducer())
                .collect();

        return buildArrayFromKeys(vertexKeys);
    }

    /**
     * Takes any given graph and creates a JSONArray containing the edge property keys and a boolean,
     * specifying it the property has a numerical type.
     *
     * @param graph input graph
     * @return  JSON array with property keys and boolean, that is true if the property type is
     * numercial
     * @throws Exception if the collecting of the distributed data fails
     */
    private JSONArray getEdgeKeys(LogicalGraph graph) throws Exception {

        List<Tuple3<Set<String>, String, Boolean>> edgeKeys = graph.getEdges()
                .flatMap(new PropertyKeyMapper<>())
                .groupBy(1)
                .reduceGroup(new LabelGroupReducer())
                .collect();

        return buildArrayFromKeys(edgeKeys);
    }

    /**
     * Convenience method.
     * Takes a set of tuples of property keys and booleans, specifying if the property is numerical,
     * and creates a JSON array containing the same data.
     *
     * @param keys set of tuples of property keys and booleans, that are true if the property type
     *             is numerical
     * @return JSONArray containing the same data as the input
     * @throws JSONException if the construction of the JSON fails
     */
    private JSONArray buildArrayFromKeys(List<Tuple3<Set<String>, String, Boolean>> keys)
            throws JSONException {
        JSONArray keyArray = new JSONArray();
        for(Tuple3<Set<String>, String, Boolean> key : keys) {
            JSONObject keyObject = new JSONObject();
            JSONArray labels = new JSONArray();
            key.f0.forEach(labels::put);
            keyObject.put("labels", labels);
            keyObject.put("name", key.f1);
            keyObject.put("numerical", key.f2);
            keyArray.put(keyObject);
        }
        return keyArray;
    }

    /**
     * Compute the labels of the vertices.
     *
     * @param graph logical graph
     * @return JSONArray containing the vertex labels
     * @throws Exception if the computation fails
     */
    private JSONArray getVertexLabels(LogicalGraph graph) throws Exception {
        List<Set<String>> vertexLabels = graph.getVertices()
                .map(new LabelMapper<>())
                .reduce(new LabelReducer())
                .collect();

        if(vertexLabels.size() > 0) {
            return buildArrayFromLabels(vertexLabels.get(0));
        } else {
            return new JSONArray();
        }
    }

    /**
     * Compute the labels of the edges.
     *
     * @param graph logical graph
     * @return JSONArray containing the edge labels
     * @throws Exception if the computation fails
     */
    private JSONArray getEdgeLabels(LogicalGraph graph ) throws Exception {
        List<Set<String>> edgeLabels = graph.getEdges()
                .map(new LabelMapper<>())
                .reduce(new LabelReducer())
                .collect();

        if(edgeLabels.size() > 0) {
            return buildArrayFromLabels(edgeLabels.get(0));
        } else {
            return new JSONArray();
        }
    }

    /**
     * Create a JSON array from the sets of labels.
     *
     * @param labels set of labels
     * @return JSON array of labels
     */
    private JSONArray buildArrayFromLabels(Set<String> labels) {
        JSONArray labelArray = new JSONArray();
        labels.forEach(labelArray::put);
        return labelArray;
    }

    public LogicalGraph readLogicalGraph(String path) {
        File[] files = new File(path).listFiles();
        boolean isJson = false;
        for(File f : files) {
            if(f.getName().contains("vertices.json")) {
                isJson = true;
                break;
            }
        }
        //File[] listOfFiles = folder.listFiles();
        DataSource source = null;
        if(isJson) {
            source = new JSONDataSource(path, config);
        } else {
            source = new CSVDataSource(path, config);
        }
        try {
            return source.getLogicalGraph();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Get the complete graph in cytoscape-conform form.
     *
     * @param databaseName name of the database
     * @return Response containing the graph as a JSON, in cytoscape conform format.
     * @throws JSONException if JSON creation fails
     * @throws IOException if reading fails
     */
    @POST
    @Path("/map/{databaseName}")
    @Produces("application/json;charset=utf-8")
    public Response getMap(@PathParam("databaseName") String databaseName) throws Exception {
        String path = RequestHandler.class.getResource("/data/" + databaseName.replace("-","/")).getPath();
        LogicalGraph graph = readLogicalGraph(path);
        GraphHead ghead = graph.getGraphHead().collect().get(0);
        List<Vertex> lv = graph.getVertices().collect();
        List<Edge>   le = graph.getEdges().collect();
        String json = CytoJSONBuilder.getJSON(ghead,lv,le,new Vector<>());
        return Response.ok(json).header("Access-Control-Allow-Origin","*").build();
    }

    /**
     * Get the complete graph in cytoscape-conform form.
     *
     * @param databaseName name of the database
     * @return Response containing the graph as a JSON, in cytoscape conform format.
     * @throws JSONException if JSON creation fails
     * @throws IOException if reading fails
     */
    @POST
    @Path("/graph/{databaseName}")
    @Produces("application/json;charset=utf-8")
    public Response getGraph(@PathParam("databaseName") String databaseName) throws Exception {
        String[] splitted = databaseName.split(",");
        String dbname = splitted[0].replace("-","/");//databaseName.substring(0,databaseName.indexOf(","));
        String sampling = splitted[1];//databaseName.substring(databaseName.indexOf(",") + 1, databaseName.lastIndexOf(","));
        float samplingThreshold = Float.parseFloat(splitted[2]);
        String layout = splitted[3];
        int maxIter = Integer.parseInt(splitted[4]);
        String[] clusterSize = splitted[5].split("-");
        int min = Integer.parseInt(clusterSize[0]);
        int max = Integer.parseInt(clusterSize[1]);
        String clusterId = splitted[6];

        String path = RequestHandler.class.getResource("/data/" + dbname).getPath();
        LogicalGraph graph = readLogicalGraph(path);

        Vector<Pair<String,String>> hm = new Vector<>();

        hm.add(new Pair<>("Vertex count",graph.getVertices().count()+""));
        hm.add(new Pair<>("Edge count",graph.getEdges().count()+""));

//      Graph<GradoopId, Double, Double> gellyGraph = GradoopGelly.gradoopToGellyForPageRank(graph);
//      double DAMPENING_FACTOR = 0.85;
//      gellyGraph.run(new PageRank<>(DAMPENING_FACTOR, 100));
//      LogicalGraph.fromDataSets(
//              labeledVertices, logicalGraph.getEdges(), logicalGraph.getConfig());
        List<Tuple2<String,Integer>> ret =  new Vector<>();
        graph = samplings.sample(graph,sampling,samplingThreshold,hm,min,max,clusterId,ENV);

        hm.add(new Pair<>("Vertex count after sampling",graph.getVertices().count()+""));
        hm.add(new Pair<>("Edge count after sampling",graph.getEdges().count()+""));

        GraphHead ghead = graph.getGraphHead().collect().get(0);
        List<Vertex> lv = graph.getVertices().collect();
        List<Edge>   le = graph.getEdges().collect();
        NeighborhoodGraph ng = new NeighborhoodGraph(lv,le);
        //lv = ng.filterDisconnectedVertices();
        hm.add(new Pair<>("Vertex count after removing disconnected vertices",lv.size()+""));
        if(layout.equals("FR layout on server")) ng.forceDirected2(lv,le,maxIter);
        else if(layout.equals("Compound layout on server")) ng.forceDirectedCluster(lv,le,maxIter);
        String json = CytoJSONBuilder.getJSON(ghead,lv,le,hm);
        return Response.ok(json).header("Access-Control-Allow-Origin","*").build();
    }

    /**
     * Takes a {@link GroupingRequest}, executes a grouping with the parameters it contains and
     * returns the results as a JSON.
     *
     * @param request GroupingRequest send to the server, containing the parameters for a
     *        {@link Grouping}.
     * @return a JSON containing the result of the executed Grouping, a graph
     * @throws Exception if the collecting of the distributed data fails
     */
    @POST
    @Path("/data")
    @Produces("application/json;charset=utf-8")
    public Response getData(GroupingRequest request) throws Exception {

        //load the database
        String databaseName = request.getDbName();

        String path = RequestHandler.class.getResource("/data/" + databaseName).getPath();

        CSVDataSource source = new CSVDataSource(path, config);

        LogicalGraph graph = source.getLogicalGraph();

        //if no edges are requested, remove them as early as possible
        //else, apply the normal filters
        if(request.getFilterAllEdges()) {
            graph = graph.subgraph(new LabelFilter<>(request.getVertexFilters()),
                    new AcceptNoneFilter<>());
        } else{
            graph = graph.subgraph(new LabelFilter<>(request.getVertexFilters()),
                    new LabelFilter<>(request.getEdgeFilters()));
        }


        //construct the grouping with the parameters send by the request
        Grouping.GroupingBuilder builder = new Grouping.GroupingBuilder();
        int position;
        position = ArrayUtils.indexOf(request.getVertexKeys(), "label");
        if(position > -1) {
            builder.useVertexLabel(true);
            request.setVertexKeys((String[])ArrayUtils.remove(request.getVertexKeys(), position));
        }
        builder.addVertexGroupingKeys(Arrays.asList(request.getVertexKeys()));


        position = ArrayUtils.indexOf(request.getEdgeKeys(), "label");
        if(position > -1) {
            builder.useEdgeLabel(true);
            request.setEdgeKeys((String[])ArrayUtils.remove(request.getEdgeKeys(), position));
        }
        builder.addEdgeGroupingKeys(Arrays.asList(request.getEdgeKeys()));

        String[] vertexAggrFuncs = request.getVertexAggrFuncs();

        for(String vertexAggrFunc : vertexAggrFuncs) {
            String[] split = vertexAggrFunc.split(" ");
            switch (split[0]) {
                case "max":
                    builder.addVertexAggregator(new MaxAggregator(split[1], "max " + split[1]));
                    break;
                case "min":
                    builder.addVertexAggregator(new MinAggregator(split[1], "min " + split[1]));
                    break;
                case "sum":
                    builder.addVertexAggregator(new SumAggregator(split[1], "sum " + split[1]));
                    break;
                case "count":
                    builder.addVertexAggregator(new CountAggregator());
                    break;
            }
        }

        String[] edgeAggrFuncs = request.getEdgeAggrFuncs();

        for(String edgeAggrFunc : edgeAggrFuncs) {
            String[] split = edgeAggrFunc.split(" ");
            switch (split[0]) {
                case "max":
                    builder.addEdgeAggregator(new MaxAggregator(split[1], "max " + split[1]));
                    break;
                case "min":
                    builder.addEdgeAggregator(new MinAggregator(split[1], "min " + split[1]));
                    break;
                case "sum":
                    builder.addEdgeAggregator(new SumAggregator(split[1], "sum " + split[1]));
                    break;
                case "count":
                    builder.addEdgeAggregator(new CountAggregator());
                    break;
            }
        }

        // by default, we use the group reduce strategy
        builder.setStrategy(GroupingStrategy.GROUP_REDUCE);

        graph = builder.build().execute(graph);

        // specify the output collections
        List<GraphHead> resultHead = new ArrayList<>();
        List<Vertex> resultVertices = new ArrayList<>();
        List<Edge> resultEdges = new ArrayList<>();

        graph.getGraphHead().output(new LocalCollectionOutputFormat<>(resultHead));

        graph.getVertices().output(new LocalCollectionOutputFormat<>(resultVertices));

        graph.getEdges().output(new LocalCollectionOutputFormat<>(resultEdges));

        try {
            ENV.execute();

            // build the response JSON from the collections

            String json = CytoJSONBuilder.getJSON(resultHead.get(0), resultVertices, resultEdges, new Vector<>());

            return Response.ok(json).header("Access-Control-Allow-Origin","*").build();

        } catch (Exception e) {
            e.printStackTrace();
            // if any exception is thrown, return an error to the client
            return Response.serverError().build();
        }
    }
}