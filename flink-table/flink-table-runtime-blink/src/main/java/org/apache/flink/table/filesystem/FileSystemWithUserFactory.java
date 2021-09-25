package org.apache.flink.table.filesystem;

import org.apache.flink.core.fs.FileSystem;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;

public interface FileSystemWithUserFactory extends Serializable {

	FileSystem create(URI fsUri, String user) throws IOException;

}
