package api.movie.data;

import api.movie.model.Movie;
import api.movie.model.Rating;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private final String[] MOVIES_FIELD_NAMES= new String[]{
            "tconst", "titleType", "primaryTitle", "runtimeMinutes", "genres"
    };

    private final String[] RATINGS_FIELD_NAMES= new String[]{
            "tconst", "averageRating", "numVotes"
    };

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public FlatFileItemReader<MovieInput> movieReader() {
        return new FlatFileItemReaderBuilder<MovieInput>().name("MovieItemReader")
                .resource(new ClassPathResource("movies.csv"))
                .delimited()
                .names(MOVIES_FIELD_NAMES)
                .fieldSetMapper(new BeanWrapperFieldSetMapper<MovieInput>() {{
                    setTargetType(MovieInput.class);
                }})
                .build();
    }

    @Bean
    public FlatFileItemReader<RatingInput> ratingReader() {
        return new FlatFileItemReaderBuilder<RatingInput>().name("RatingItemReader")
                .resource(new ClassPathResource("ratings.csv"))
                .delimited()
                .names(RATINGS_FIELD_NAMES)
                .fieldSetMapper(new BeanWrapperFieldSetMapper<RatingInput>() {{
                    setTargetType(RatingInput.class);
                }})
                .build();
    }

    @Bean
    public MovieDataProcessor movieProcessor() {
        return new MovieDataProcessor();
    }

    @Bean
    public RatingDataProcessor ratingProcessor() {
        return new RatingDataProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Movie> movieWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Movie>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO movie (tconst, title_type, primary_title, runtime_minutes, genres) " +
                        "VALUES (:tconst, :titleType, :primaryTitle, :runtimeMinutes, :genres)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Rating> ratingWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Rating>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO rating (tconst, average_rating, num_votes) " +
                        "VALUES (:tconst, :averageRating, :numVotes)")
                .dataSource(dataSource)
                .build();
    }

//    @Bean
//    public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
//        return jobBuilderFactory.get("importUserJob")
//                .incrementer(new RunIdIncrementer())
//                .listener(listener)
//                .flow(step1)
//                .end()
//                .build();
//    }

    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step movieStep, Step ratingStep) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(movieStep)
                .next(ratingStep)
                .end()
                .build();
    }

    @Bean
    public Step movieStep(JdbcBatchItemWriter<Movie> writer) {
        return stepBuilderFactory.get("movieStep")
                .<MovieInput, Movie> chunk(10)
                .reader(movieReader())
                .processor(movieProcessor())
                .writer(writer)
                .build();
    }

    @Bean
    public Step ratingStep(JdbcBatchItemWriter<Rating> writer) {
        return stepBuilderFactory.get("ratingStep")
                .<RatingInput, Rating> chunk(10)
                .reader(ratingReader())
                .processor(ratingProcessor())
                .writer(writer)
                .build();
    }


}