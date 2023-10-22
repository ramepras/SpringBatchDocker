package com.play.tags;

import java.sql.Timestamp;

public record Tag(Long userId, Long movieId, String tag, Timestamp timestamp) {
}
