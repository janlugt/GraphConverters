package graphconverter.writers;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import util.Graph;

public class AvroWriter {

	private final static String DEST_ID = "dest_id", DEST_VAL = "dest_value";
	private final static String SRC_ID = "src_id", SRC_VAL = "src_val", EDGE_LIST = "edge_list";
	
	public static void write(Graph graph, String filename) throws IOException {
		Schema vertexSchema = new Schema.Parser().parse(new File("src/main/avro/gm_avro_graph.avpr"));
		Schema edgelistSchema = vertexSchema.getField(EDGE_LIST).schema().getTypes().get(1);
		Schema edgeSchema = edgelistSchema.getElementType().getTypes().get(1);
		
		DataFileWriter<GenericRecord> dfw = null;
		try {
			dfw = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>()).create(vertexSchema, new File(filename));
			for (int src : graph.keySet()) {
				List<Integer> dests = graph.get(src);
				
				GenericArray<GenericRecord> edgelist = new GenericData.Array<GenericRecord>(dests.size(), edgelistSchema);
				for (int i = 0; i < dests.size(); i++) {
					GenericRecord edge = new GenericData.Record(edgeSchema);
					edge.put(DEST_ID, (long) dests.get(i));
					edge.put(DEST_VAL, null);
					edgelist.add(edge);
				}
				GenericRecord vertex = new GenericData.Record(vertexSchema);
				vertex.put(SRC_ID, (long) src);
				vertex.put(SRC_VAL, null);
				vertex.put(EDGE_LIST, edgelist);
				dfw.append(vertex);
			}
		} finally {
			dfw.close();
		}
	}
}
