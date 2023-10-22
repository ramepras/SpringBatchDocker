package com.play.tags;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.validation.BindException;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

@SpringBootApplication
public class TagBatchApplication {
    public static void main(String[] args) {
        SpringApplication.run(TagBatchApplication.class, args);
    }

    @Bean
    Job job(TagBatchConfig tagBatchConfig) {
        return new JobBuilder("JobCsvTagToDb", tagBatchConfig.jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(tagBatchConfig.csvTagToDb())
                .build();
    }

    @Configuration
    class TagBatchConfig {
        private JobRepository jobRepository;
        private PlatformTransactionManager platformTransactionManager;
        private DataSource dataSource;

        public TagBatchConfig(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager, DataSource dataSource) {
            this.jobRepository = jobRepository;
            this.platformTransactionManager = platformTransactionManager;
            this.dataSource = dataSource;
        }

        @Bean
        FlatFileItemReader<Tag> tagReader() {
            return new FlatFileItemReaderBuilder<Tag>()
                    .name("csvTagReader")
                    .resource(new ClassPathResource("movielens-tags-smallfile.csv"))
                    .delimited().delimiter(",")
                    .names("userId", "movieId", "tag", "timestamp")
                    .linesToSkip(1)
                    .fieldSetMapper(new FieldSetMapper<Tag>() {
                        @Override
                        public Tag mapFieldSet(FieldSet fieldSet) throws BindException {
                            long ts = fieldSet.readLong("timestamp");
                            long epochMilli = Instant.now().toEpochMilli();
                            Timestamp timestamp = (isNotNullNotEmpty(String.valueOf(ts))) ? new Timestamp(ts) : new Timestamp(epochMilli); // if null then keep current time.
                            return new Tag(fieldSet.readLong("userId"),
                                    fieldSet.readLong("movieId"),
                                    fieldSet.readString("tag"),
                                    timestamp);
                        }
                    })
                    .build();

        }

        @Bean
        JdbcBatchItemWriter<Tag> tagWriter() {
            String sql = "INSERT INTO tag (user_id, movie_id, tag, timestamp) VALUES (?,?,?,?)";
            return new JdbcBatchItemWriterBuilder<Tag>()
                    .sql(sql)
                    .dataSource(dataSource)
                    .itemPreparedStatementSetter(new ItemPreparedStatementSetter<Tag>() {
                        @Override
                        public void setValues(Tag item, PreparedStatement ps) throws SQLException {
                            ps.setLong(1, item.userId());
                            ps.setLong(2, item.movieId());
                            ps.setString(3, item.tag());
                            ps.setTimestamp(4, item.timestamp());
                        }
                    })
                    .build();

        }

        @Bean
        Step csvTagToDb() {
            return new StepBuilder("StepCsvToDb", jobRepository)
                    .<Tag, Tag>chunk(100, platformTransactionManager)
                    .reader(tagReader())
                    .writer(tagWriter())
                    .build();
        }

        private static boolean isNotNullNotEmpty(String str) {
            return str != null && !str.trim().isEmpty();
        }
    }
}
