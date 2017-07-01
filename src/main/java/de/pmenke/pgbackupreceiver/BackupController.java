package de.pmenke.pgbackupreceiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by pmenke on 01.07.17.
 */
@Controller
@ResponseBody
public class BackupController {
    private static final Logger LOG                      = LoggerFactory.getLogger(BackupController.class);
    private static final  FileAttribute<Set<PosixFilePermission>> PERMISSION750 =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---"));
    private static final  FileAttribute<Set<PosixFilePermission>> PERMISSION640 =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r-----"));
    private static final String INSTANCE_NAME_PATTERN    = "^[a-zA-Z-_0-9][a-zA-Z-_.0-9]*$";
    private static final String WAL_SEGMENT_NAME_PATTERN = "^[a-zA-Z-_0-9][a-zA-Z-_.0-9]*$";
    private final Path backupPath;

    public BackupController(final String backupPath) {
        this.backupPath = Paths.get(backupPath);
        if (!Files.exists(this.backupPath)) {
            throw new IllegalArgumentException("Backup path \"" + backupPath + "\" does not exist");
        }
    }

    @RequestMapping(path = "/base_backup/{instance}", method = RequestMethod.PUT, consumes = "application/octet-stream")
    public ResponseEntity<?> putBaseBackup(final @PathVariable("instance") String instance,
                                           final @RequestParam("date") 
                                                 @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime date,
                                           final HttpServletRequest request) {
        if (!instance.matches(INSTANCE_NAME_PATTERN)) {
            return ResponseEntity.badRequest().body("Invalid instance name");
        }
        
        final Path backupFile;
        try {
            backupFile = getStorageDir(instance, "base_backup")
                         .resolve(date.truncatedTo(ChronoUnit.SECONDS).toString());
            if (Files.exists(backupFile)) {
                return ResponseEntity.badRequest().body("Backup with the given name already exists");
            }
            try(final ServletInputStream inputStream = request.getInputStream()){
                Files.copy(inputStream, backupFile);
            }
            Files.setPosixFilePermissions(backupFile, PERMISSION640.value());
        } catch (IOException e) {
            LOG.error("IO-Error while persisting backup", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("IO-Error while persisting backup");
        }

        return ResponseEntity.created(URI.create("./" + backupFile.getFileName())).build();
    }

    @RequestMapping(path = "/base_backup/{instance}", method = RequestMethod.GET)
    public ResponseEntity<?> listBaseBackups(final @PathVariable("instance") String instance) {
        if (!instance.matches(INSTANCE_NAME_PATTERN)) {
            return ResponseEntity.badRequest().body("Invalid instance name");
        }

        try {
            final Path instanceBackupPath = getStorageDir(instance, "base_backup");
            final List<String> backups = Files.list(instanceBackupPath)
                    .filter(((Predicate<Path>) Files::isDirectory).negate())
                    .map(path -> path.getFileName().toString())
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
            return ResponseEntity.ok(backups);
        } catch (IOException e) {
            LOG.error("IO-Error while listing backups", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("IO-Error while listing backups");
        }
    }

    @RequestMapping(path = "/wal_segment/{instance}/{name}", method = RequestMethod.PUT, consumes = "application/octet-stream")
    public ResponseEntity<?> putWalSegment(final @PathVariable("instance") String instance,
                                           final @PathVariable("name") String name,
                                           final HttpServletRequest request) {
        if (!instance.matches(INSTANCE_NAME_PATTERN)) {
            return ResponseEntity.badRequest().body("Invalid instance name");
        }

        if (!name.matches(WAL_SEGMENT_NAME_PATTERN)) {
            return ResponseEntity.badRequest().body("Invalid WAL segment name");
        }


        final Path segmentFile;
        try {
            segmentFile = getStorageDir(instance, "wal_segment").resolve(name);
            if (Files.exists(segmentFile)) {
                return ResponseEntity.badRequest().body("WAL segment with the given name already exists");
            }

            try(final ServletInputStream inputStream = request.getInputStream()){
                Files.copy(inputStream, segmentFile);
            }
            Files.setPosixFilePermissions(segmentFile, PERMISSION640.value());
        } catch (IOException e) {
            LOG.error("IO-Error while persisting WAL segment", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("IO-Error while persisting WAL segment");
        }

        return ResponseEntity.created(URI.create("./" + segmentFile.getFileName().toString())).build();
    }

    @RequestMapping(path = "/wal_segment/{instance}", method = RequestMethod.GET)
    public ResponseEntity<?> listWalSegments(final @PathVariable("instance") String instance) {
        if (!instance.matches(INSTANCE_NAME_PATTERN)) {
            return ResponseEntity.badRequest().body("Invalid instance name");
        }

        try {
            final Path instanceSegmentPath = getStorageDir(instance, "wal_segment");
            final List<String> segments = Files.list(instanceSegmentPath)
                    .filter(((Predicate<Path>) Files::isDirectory).negate())
                    .map(path -> path.getFileName().toString())
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
            return ResponseEntity.ok(segments);
        } catch (IOException e) {
            LOG.error("IO-Error while listing WAL segments", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("IO-Error while WAL segments");
        }
    }

    private Path getStorageDir(@PathVariable("instance") String instance, String subDir) throws IOException {
        final Path path = backupPath
                .resolve(instance)
                .resolve(subDir);
        if (!Files.exists(path)) {
            Files.createDirectories(path, PERMISSION750);
        }
        return path;
    }
}
