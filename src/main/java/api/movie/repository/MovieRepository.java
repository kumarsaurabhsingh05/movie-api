package api.movie.repository;

import api.movie.model.Movie;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public interface MovieRepository extends JpaRepository<Movie, String> {

    List<Movie> findTop10ByOrderByRuntimeMinutesDesc();

    List<Movie> findByRatingAverageRatingGreaterThanOrderByRatingAverageRatingDesc(double rating);

    @Query("SELECT m.genres AS genre, m.primaryTitle AS primaryTitle, SUM(r.numVotes) AS numVotes " +
            "FROM Movie m JOIN m.rating r " +
            "GROUP BY m.genres, m.primaryTitle " +
            "ORDER BY m.genres, m.primaryTitle")
    List<Object[]> getGenreMoviesWithSubtotals();

    @Modifying
    @Transactional
    @Query(value = "UPDATE movie m SET m.runtime_minutes = " +
            "CASE " +
            "    WHEN m.genres = 'Documentary' THEN m.runtime_minutes + 15 " +
            "    WHEN m.genres = 'Animation' THEN m.runtime_minutes + 30 " +
            "    ELSE m.runtime_minutes + 45 " +
            "END", nativeQuery = true)
    void incrementRuntimeMinutes();

}
