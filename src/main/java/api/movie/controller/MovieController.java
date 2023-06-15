package api.movie.controller;

import api.movie.dto.NewMovieDTO;
import api.movie.model.Movie;
import api.movie.repository.MovieRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class MovieController {

    @Autowired
    private MovieRepository movieRepository;

    @GetMapping("/longest-duration-movies")
    public List<Movie> getLongestDurationMovies() {
        return movieRepository.findTop10ByOrderByRuntimeMinutesDesc();
    }

    @PostMapping("/new-movie")
    public String addNewMovie(@RequestBody NewMovieDTO newMovieDTO) {
        Movie movie = new Movie();
        movie.setTconst(newMovieDTO.getTconst());
        movie.setTitleType(newMovieDTO.getTitleType());
        movie.setPrimaryTitle(newMovieDTO.getPrimaryTitle());
        movie.setRuntimeMinutes(newMovieDTO.getRuntimeMinutes());
        movie.setGenres(newMovieDTO.getGenres());

        movieRepository.save(movie);

        return "success";
    }

    @GetMapping("/top-rated-movies")
    public List<Movie> getTopRatedMovies() {
        Double rating = new Double("6.0");
        return movieRepository.findByRatingAverageRatingGreaterThanOrderByRatingAverageRatingDesc(rating);
    }

    @GetMapping("/genre-movies-with-subtotals")
    public List<Map<String, Object>> getGenreMoviesWithSubtotals() {
        List<Object[]> results = movieRepository.getGenreMoviesWithSubtotals();
        List<Map<String, Object>> response = new ArrayList<>();

        String currentGenre = null;
        Long genreTotal = 0L;
        Map<String, Object> genreMap = null;
        for (Object[] result : results) {
            String genre = (String) result[0];
            String primaryTitle = (String) result[1];
            Long numVotes = (Long) result[2];

            if (!genre.equals(currentGenre)) {
                if (genreMap != null) {
                    genreMap.put("TOTAL", genreTotal);
                    response.add(genreMap);
                }

                genreMap = new HashMap<>();
                genreMap.put("Genre", genre);
                genreMap.put("Movies", new ArrayList<Map<String, Object>>());
                currentGenre = genre;
                genreTotal = 0L;
            }

            Map<String, Object> movieMap = new HashMap<>();
            movieMap.put("primaryTitle", primaryTitle);
            movieMap.put("numVotes", numVotes);

            ((List<Map<String, Object>>) genreMap.get("Movies")).add(movieMap);
            genreTotal += numVotes;
        }

        if (genreMap != null) {
            genreMap.put("TOTAL", genreTotal);
            response.add(genreMap);
        }

        return response;
    }

    @PostMapping("/update-runtime-minutes")
    public String updateRuntimeMinutes() {
        movieRepository.incrementRuntimeMinutes();
        return "Success";
    }

}

