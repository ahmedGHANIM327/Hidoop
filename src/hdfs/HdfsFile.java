package hdfs;

import java.io.Serializable;
import java.util.Map;

import formats.Format;

public class HdfsFile implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private String filename;
    private Map<String, HdfsServer> chunksDistribution;
    private Integer ChunksNumber;
    private Format.Type filetype;
    
    public HdfsFile (String name, Map<String, HdfsServer> cd, Integer cn, Format.Type t) {
        this.filename = name;
        this.chunksDistribution = cd;
        this.ChunksNumber = cn;
        this.filetype = t;
    }

    public String getName() {
        return filename;
    }

    public Map<String, HdfsServer> getDistribution() {
        return chunksDistribution;
    }

    public Integer getChunksNumber() {
        return ChunksNumber;
    }

    public Format.Type getType() {
        return filetype;
    }

}
