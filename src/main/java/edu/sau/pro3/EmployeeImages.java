package edu.sau.pro3;

import jakarta.annotation.PreDestroy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.imageio.ImageIO;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Service
class EmployeeImages {
    private final URI uri;
    private final Path directory;
    private final Configuration configuration = new Configuration();
    private FileSystem filesystem;

    EmployeeImages(@Value("${pro3.hdfs.uri}") URI uri, @Value("${pro3.hdfs.directory}") String directory) {
        this.uri = uri;
        this.directory = new Path(directory);
    }

    Set<String> photoNames() throws IOException {
        var names = new HashSet<String>();
        try {
            var files = filesystem().listFiles(directory, false);
            while (files.hasNext()) {
                names.add(files.next().getPath().getName());
            }
        } catch (FileNotFoundException missing) {
            return Set.of();
        }
        return names;
    }

    Image read(String name) throws IOException {
        var fs = filesystem();
        try (var input = fs.open(path(name))) {
            return new Image(input.readAllBytes(), MediaType.IMAGE_JPEG);
        } catch (FileNotFoundException missing) {
            try (var input = fs.open(new Path(directory, "placeholder.png"))) {
                return new Image(input.readAllBytes(), MediaType.IMAGE_PNG);
            }
        }
    }

    void store(String name, MultipartFile upload) throws IOException {
        if (upload.isEmpty()) {
            throw new IllegalArgumentException("Choose a JPEG file.");
        }
        try (var raw = upload.getInputStream(); var image = ImageIO.createImageInputStream(raw)) {
            var readers = ImageIO.getImageReaders(image);
            if (!readers.hasNext()) {
                throw new IllegalArgumentException("Choose a valid JPEG file.");
            }
            var reader = readers.next();
            try {
                reader.setInput(image);
                if (!reader.getFormatName().equalsIgnoreCase("JPEG")
                        || reader.getWidth(0) > 4096 || reader.getHeight(0) > 4096 || reader.read(0) == null) {
                    throw new IllegalArgumentException("Choose a JPEG no larger than 4096 × 4096 pixels.");
                }
            } catch (IOException damaged) {
                throw new IllegalArgumentException("The JPEG file could not be read.", damaged);
            } finally {
                reader.dispose();
            }
        }
        var fs = filesystem();
        fs.mkdirs(directory);
        Path temporary = new Path(directory, "." + UUID.randomUUID() + ".upload");
        try {
            try (var input = upload.getInputStream(); var output = fs.create(temporary, false)) {
                input.transferTo(output);
            }
            FileContext.getFileContext(uri, configuration).rename(temporary, path(name), Options.Rename.OVERWRITE);
        } finally {
            fs.delete(temporary, false);
        }
    }

    private Path path(String name) {
        if (!name.matches("[a-zA-Z0-9_-]+\\.jpg")) {
            throw new IllegalArgumentException("Invalid employee photo filename.");
        }
        return new Path(directory, name);
    }

    private synchronized FileSystem filesystem() throws IOException {
        if (filesystem == null) {
            filesystem = FileSystem.newInstance(uri, configuration);
        }
        return filesystem;
    }

    @PreDestroy
    synchronized void close() throws IOException {
        if (filesystem != null) {
            filesystem.close();
        }
    }

    record Image(byte[] bytes, MediaType mediaType) {}
}
