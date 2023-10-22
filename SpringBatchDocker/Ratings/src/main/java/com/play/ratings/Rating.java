package com.play.ratings;

import java.sql.Timestamp;

public record Rating(Long userId, Long movieId, Double rating, Timestamp timestamp) {
}
