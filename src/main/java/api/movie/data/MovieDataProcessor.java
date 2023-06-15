package api.movie.data;

import api.movie.model.Movie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;

public class MovieDataProcessor implements ItemProcessor<MovieInput, Movie> {

    private static final Logger log = LoggerFactory.getLogger(MovieDataProcessor.class);

    @Override
    public Movie process(final MovieInput movieInput) throws Exception {

        Movie movie = new Movie();

        movie.setTconst(movieInput.getTconst());
        movie.setTitleType(movieInput.getTitleType());
        movie.setPrimaryTitle(movieInput.getPrimaryTitle());
        movie.setRuntimeMinutes(movieInput.getRuntimeMinutes());
        movie.setGenres(movieInput.getGenres());

        return movie;

    }

}