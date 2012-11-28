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

	public static void write(Graph graph, String filename) throws IOException {
		Schema vertexSchema = new Schema.Parser().parse(new File("src/main/avro/gm_avro_graph.avpr"));
		Schema edgelistSchema = vertexSchema.getField("edge_list").schema().getTypes().get(1);
		Schema edgeSchema = edgelistSchema.getElementType().getTypes().get(1);
		
		DataFileWriter<GenericRecord> dfw = null;
		try {
			dfw = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>()).create(vertexSchema, new File(filename));
			for (int src : graph.keySet()) {
				List<Integer> dests = graph.get(src);
				
				GenericArray<GenericRecord> edgelist = new GenericData.Array<GenericRecord>(dests.size(), edgelistSchema);
				for (int i = 0; i < dests.size(); i++) {
					GenericRecord edge = new GenericData.Record(edgeSchema);
					edge.put("dest_id", (long) dests.get(i));
					edge.put("dest_value", null);
					edgelist.add(edge);
				}
				GenericRecord vertex = new GenericData.Record(vertexSchema);
				vertex.put("src_id", (long) src);
				vertex.put("src_val", null);
				vertex.put("edge_list", edgelist);
				dfw.append(vertex);
			}
		} finally {
			dfw.close();
		}
	}
}
