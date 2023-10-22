package com.play.ratings;

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
public class RatingBatchApplication {
    public static void main(String[] args) {
        SpringApplication.run(RatingBatchApplication.class, args);
    }

    @Bean
    Job job(RatingBatchConfig ratingBatchConfig) {
        return new JobBuilder("jobCsvRatingToDb", ratingBatchConfig.jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(ratingBatchConfig.csvRatingToDb())
                .build();
    }

    @Configuration
    class RatingBatchConfig {
        private JobRepository jobRepository;
        private PlatformTransactionManager platformTransactionManager;
        private DataSource dataSource;

        public RatingBatchConfig(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager, DataSource dataSource) {
            this.jobRepository = jobRepository;
            this.platformTransactionManager = platformTransactionManager;
            this.dataSource = dataSource;
        }

        @Bean
        FlatFileItemReader<Rating> ratingReader() {
            return new FlatFileItemReaderBuilder<Rating>()
                    .name("csvRatingReader")
                    .resource(new ClassPathResource("movielens-ratings-smallfile.csv"))
                    .delimited().delimiter(",")
                    .names("userId", "movieId", "rating", "timestamp")
                    .linesToSkip(1)
                    .fieldSetMapper(new FieldSetMapper<Rating>() {
                        @Override
                        public Rating mapFieldSet(FieldSet fieldSet) throws BindException {
                            long timestamp = fieldSet.readLong("timestamp");
                            long epochMilli = Instant.now().toEpochMilli();
                            Timestamp ts = (isNotNullNotEmpty(String.valueOf(timestamp))) ? new Timestamp(timestamp) : new Timestamp(epochMilli);
                            return new Rating(fieldSet.readLong("userId"),
                                    fieldSet.readLong("movieId"),
                                    fieldSet.readDouble("rating"),
                                    ts);
                        }
                    })
                    .build();
        }

        @Bean
        JdbcBatchItemWriter<Rating> ratingWriter() {
            String sql = "insert into rating (user_id,movie_id,rating,timestamp) VALUES (?,?,?,?)";
            return new JdbcBatchItemWriterBuilder<Rating>()
                    .sql(sql)
                    .dataSource(dataSource)
                    .itemPreparedStatementSetter(new ItemPreparedStatementSetter<Rating>() {
                        @Override
                        public void setValues(Rating item, PreparedStatement ps) throws SQLException {
                            ps.setLong(1, item.userId());
                            ps.setLong(2, item.movieId());
                            ps.setDouble(3, item.rating());
                            ps.setTimestamp(4, item.timestamp());
                        }
                    })
                    .build();
        }

        @Bean
        Step csvRatingToDb() {
            return new StepBuilder("StepCsvRatingToDb", jobRepository)
                    .<Rating, Rating>chunk(100, platformTransactionManager)
                    .reader(ratingReader())
                    .writer(ratingWriter())
                    .build();
        }

        private static boolean isNotNullNotEmpty(String str) {
            return str != null && !str.trim().isEmpty();
        }
    }
}
