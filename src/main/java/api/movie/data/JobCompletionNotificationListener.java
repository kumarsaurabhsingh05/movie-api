package api.movie.data;

import api.movie.model.Movie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("Job is about to start: " + jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED! Time to verify the results");

            log.info("Movie Results:");
            jdbcTemplate.query("SELECT title_type, primary_title FROM movie",
                    (rs, row) -> "Title Type : " + rs.getString(1) + " Primary Title : " + rs.getString(2)
            ).forEach(System.out::println);

            log.info("Rating Results:");
            jdbcTemplate.query("SELECT tconst, num_votes FROM rating",
                    (rs, row) -> "T_Const : " + rs.getString(1) + " Number of Votes : " + rs.getString(2)
            ).forEach(System.out::println);
        }
    }
}