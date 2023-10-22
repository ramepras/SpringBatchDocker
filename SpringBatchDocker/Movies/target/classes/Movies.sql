DROP DATABASE IF EXISTS `Movies`;
CREATE DATABASE `Movies`;
USE `Movies`;
CREATE TABLE Movie (
    movie_id BIGINT PRIMARY KEY,
    movie_title VARCHAR(255),
    movie_year BIGINT,
    movie_genres VARCHAR(255)
);
#-----------------------------
select COUNT(*) from movie;
select COUNT(*) from rating;
select * from movie limit 10;
SELECT * FROM Movie WHERE movie_year = 2001 LIMIT 10;

select * from movie where movie_id = 160527;

select count(rating) from rating where movie_id = 4148;

SELECT AVG(rating) AS average_rating
FROM rating
WHERE movie_id = 4148;

SELECT movie_id, AVG(rating) AS average_rating
FROM rating
GROUP BY movie_id
HAVING AVG(rating) > 4.5;

# count movies which has average rating 5

SELECT COUNT(*) AS count_of_records
FROM (
    SELECT movie_id, AVG(rating) AS average_rating
    FROM rating
    GROUP BY movie_id
) AS average_ratings
WHERE average_rating = 5.0;




SELECT r.movie_id, m.movie_title, AVG(r.rating) AS average_rating
FROM rating r
JOIN movie m ON r.movie_id = m.movie_id
GROUP BY r.movie_id, m.movie_title
HAVING AVG(r.rating) = 5;


SELECT r.movie_id, m.movie_title, AVG(r.rating) AS average_rating
FROM rating r
JOIN movie m ON r.movie_id = m.movie_id
GROUP BY r.movie_id, m.movie_title
HAVING AVG(r.rating) = 5
ORDER BY m.movie_title
LIMIT 10;



SELECT COUNT(*) AS movie_count
FROM Movie
WHERE movie_year = '2009';

