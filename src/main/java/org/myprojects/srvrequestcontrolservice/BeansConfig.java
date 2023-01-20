package org.myprojects.srvrequestcontrolservice;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.myprojects.srvrequestcontrolservice.utils.SimpleCache;
import org.myprojects.srvrequestcontrolservice.utils.TempCache;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;

@EnableScheduling
@Configuration
@ComponentScan
public class BeansConfig {

    @Bean
    SimpleCache<XmlRequestTemplate> templateCache() {
        return new SimpleCache<>("templateCache");
    }

    @Bean(name = "savedRequestCache")
    TempCache<TempCache.Unit<ParsedXmlRequest>> savedRequestCache(
            @Value("${service.client-attrs-cache.time}") long cacheTimePeriod) {
        return new TempCache<>("savedRequestCache", cacheTimePeriod);
    }

    @Bean
    public DataSource dataSource(@Value("${spring.datasource.driver-class-name}") String dbDriver,
                                 @Value("${spring.datasource.url}") String dbUrl,
                                 @Value("${spring.datasource.username}") String dbUsername,
                                 @Value("${spring.datasource.password}") String dbPassword) {
        HikariConfig config = new HikariConfig();
        HikariDataSource dataSource;

        config.setDriverClassName(dbDriver);
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUsername);
        config.setPassword(dbPassword);
        dataSource = new HikariDataSource(config);

        return dataSource;
    }
}
