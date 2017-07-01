package de.pmenke.pgbackupreceiver;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Created by pmenke on 01.07.17.
 */
@Configuration
@EnableAutoConfiguration
public class SpringBootConfig {
 
    @Autowired
    private Environment env;
    
    @Bean
    public BackupController backupController() {
        return new BackupController(env.getRequiredProperty("backup.basePath"));
    }
}
