package api.movie.data;

import api.movie.model.Rating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class RatingDataProcessor implements ItemProcessor<RatingInput, Rating> {

    private static final Logger log = LoggerFactory.getLogger(MovieDataProcessor.class);

    @Override
    public Rating process(final RatingInput ratingInput) throws Exception {

        Rating rating = new Rating();

        rating.setTconst(ratingInput.getTconst());
        rating.setAverageRating(ratingInput.getAverageRating());
        rating.setNumVotes(ratingInput.getNumVotes());

        return rating;
    }

}