DROP DATABASE IF EXISTS `Ratings`;
CREATE DATABASE `Ratings`;
USE `Ratings`;
CREATE TABLE Rating (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    movie_id BIGINT NOT NULL,
    rating DOUBLE,
    timestamp TIMESTAMP
);

