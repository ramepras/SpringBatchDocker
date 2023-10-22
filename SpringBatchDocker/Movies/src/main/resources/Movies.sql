DROP DATABASE IF EXISTS `Movies`;
CREATE DATABASE `Movies`;
USE `Movies`;
CREATE TABLE Movie (
    movie_id BIGINT PRIMARY KEY,
    movie_title VARCHAR(255),
    movie_year BIGINT,
    movie_genres VARCHAR(255)
);




