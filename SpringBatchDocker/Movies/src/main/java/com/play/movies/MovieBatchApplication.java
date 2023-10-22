package com.play.movies;

import com.play.movies.util.MovieUtil;
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

@SpringBootApplication
public class MovieBatchApplication {
    public static void main(String[] args) {
        SpringApplication.run(MovieBatchApplication.class, args);
    }

    @Bean
    Job job(MovieBatchConfig movieBatchConfig) {
        return new JobBuilder("JobCsvMoviesToDb", movieBatchConfig.jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(movieBatchConfig.csvMoviesToDb())
                .build();

    }

    @Configuration
    class MovieBatchConfig {
        private JobRepository jobRepository;
        private PlatformTransactionManager platformTransactionManager;
        private DataSource dataSource;

        public MovieBatchConfig(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager, DataSource dataSource) {
            this.jobRepository = jobRepository;
            this.platformTransactionManager = platformTransactionManager;
            this.dataSource = dataSource;
        }

        @Bean
        FlatFileItemReader<Movie> movieReader() {
            return new FlatFileItemReaderBuilder<Movie>()
                    .name("csvMovieReader")
                    .resource(new ClassPathResource("movielens-movies-smallfile.csv"))
                    .delimited().delimiter(",")
                    .names("movieId", "movieTitle", "movieGenres")
                    .linesToSkip(1)
                    .fieldSetMapper(new FieldSetMapper<Movie>() {
                        @Override
                        public Movie mapFieldSet(FieldSet fieldSet) throws BindException {
                            String movieTitle = fieldSet.readString("movieTitle");
                            return new Movie(fieldSet.readLong("movieId"),
                                    MovieUtil.extractMovieTitle(movieTitle),
                                    MovieUtil.extractMovieYear(movieTitle), // year is extracted from title ...
                                    MovieUtil.extractMovieGenres(fieldSet.readString("movieGenres")));
                        }
                    })
                    .build();
        }

        @Bean
        JdbcBatchItemWriter<Movie> movieWriter() {
            String sql = "insert into movie (movie_id,movie_title,movie_year,movie_genres) values (?,?,?,?)";
            return new JdbcBatchItemWriterBuilder<Movie>()
                    .sql(sql)
                    .dataSource(dataSource)
                    .itemPreparedStatementSetter(new ItemPreparedStatementSetter<Movie>() {
                        @Override
                        public void setValues(Movie item, PreparedStatement ps) throws SQLException {
                            ps.setLong(1, item.movieId());
                            ps.setString(2, item.movieTitle());
                            ps.setLong(3, item.movieYear());
                            ps.setString(4, item.movieGenres());
                        }
                    })
                    .build();
        }

        @Bean
        Step csvMoviesToDb() {
            return new StepBuilder("StepCsvMoviesToDb", jobRepository)
                    .<Movie, Movie>chunk(100, platformTransactionManager)
                    .reader(movieReader())
                    .writer(movieWriter())
                    .build();
        }
    }
}
