DROP TABLE IF EXISTS movies;

CREATE TABLE movies (
movie_type VARCHAR (40),
director VARCHAR (100),
year_of_issue SMALLINT,
lenght_in_minutes SMALLINT,
rate FLOAT
);



CREATE TABLE movies_before_1990 (
  CHECK (year_of_issue < 1990)
) INHERITS (movies);

CREATE TABLE movies_1991_2000 (
  CHECK (year_of_issue >= 1991 AND year_of_issue < 2001)  
) INHERITS (movies);

CREATE TABLE movies_2001_2010 (
  CHECK (year_of_issue >= 2001 AND year_of_issue < 2011)
) INHERITS (movies);

CREATE TABLE movies_2011_2020 (
  CHECK (year_of_issue >= 2011 AND year_of_issue < 2021)
) INHERITS (movies);

CREATE TABLE movies_after_2021 (
  CHECK (year_of_issue >= 2021) 
) INHERITS (movies);

CREATE TABLE movies_short (
  CHECK (lenght_in_minutes <= 40)  
) INHERITS (movies);

CREATE TABLE movies_medium (
  CHECK (lenght_in_minutes >= 41 AND lenght_in_minutes <= 90)
) INHERITS (movies);

CREATE TABLE movies_long (
  CHECK (lenght_in_minutes >= 91 AND lenght_in_minutes <= 130)  
) INHERITS (movies);

CREATE TABLE movies_extra_long (
  CHECK (lenght_in_minutes > 130)
) INHERITS (movies);

CREATE TABLE movies_low_rated (
  CHECK (rate < 5)
) INHERITS (movies);

CREATE TABLE movies_medium_rated (
  CHECK (rate >= 5 AND rate < 8)   
) INHERITS (movies);

CREATE TABLE movies_high_rated (
  CHECK (rate >= 8 AND rate <= 10)
) INHERITS (movies);

CREATE RULE insert_movies_before_1990 AS 
  ON INSERT TO movies 
  WHERE (year_of_issue < 1990)
  DO INSTEAD  
    INSERT INTO movies_before_1990 VALUES (NEW.*);
    
CREATE RULE insert_movies_1991_2000 AS 
  ON INSERT TO movies 
  WHERE (year_of_issue >= 1991 AND year_of_issue < 2001)
  DO INSTEAD  
    INSERT INTO movies_1991_2000 VALUES (NEW.*);
    
CREATE RULE insert_movies_2001_2010 AS 
  ON INSERT TO movies 
  WHERE (year_of_issue >= 2001 AND year_of_issue < 2011)
  DO INSTEAD  
    INSERT INTO movies_2001_2010 VALUES (NEW.*);
    
CREATE RULE insert_movies_2011_2020 AS 
  ON INSERT TO movies 
  WHERE (year_of_issue >= 2011 AND year_of_issue < 2021)
  DO INSTEAD  
    INSERT INTO movies_2011_2020 VALUES (NEW.*);
    
CREATE RULE insert_after_2021 AS 
  ON INSERT TO movies 
  WHERE (year_of_issue > 2020)
  DO INSTEAD  
    INSERT INTO movies_after_2021 VALUES (NEW.*);
    
CREATE RULE insert_movies_low_rated AS 
   ON INSERT TO movies WHERE 
    (rate < 5)  
   DO INSTEAD  
     INSERT INTO movies_low_rated VALUES (NEW.*);
     
CREATE RULE insert_movies_medium_rated AS 
   ON INSERT TO movies WHERE 
    (rate >= 5 AND rate < 8)  
   DO INSTEAD  
     INSERT INTO movies_medium_rated VALUES (NEW.*);
     
CREATE RULE insert_movies_high_rated AS 
   ON INSERT TO movies WHERE 
    (rate >= 8 AND rate <= 10)  
   DO INSTEAD  
     INSERT INTO movies_high_rated VALUES (NEW.*);
     
CREATE RULE insert_movies_short AS 
   ON INSERT TO movies WHERE 
    (lenght_in_minutes <= 40)  
   DO INSTEAD  
     INSERT INTO movies_short VALUES (NEW.*);
     
CREATE RULE insert_movies_medium AS 
   ON INSERT TO movies WHERE 
    (lenght_in_minutes >= 41 AND lenght_in_minutes <= 90)  
   DO INSTEAD  
     INSERT INTO movies_medium VALUES (NEW.*);
     
CREATE RULE insert_movies_long AS 
   ON INSERT TO movies WHERE 
    (lenght_in_minutes >= 91 AND lenght_in_minutes <= 130)  
   DO INSTEAD  
     INSERT INTO movies_long VALUES (NEW.*);
     
CREATE RULE insert_movies_extra_long AS 
   ON INSERT TO movies WHERE 
    (lenght_in_minutes >= 131)  
   DO INSTEAD  
     INSERT INTO movies_extra_long VALUES (NEW.*);

INSERT INTO movies VALUES
  ('Драма', 'Сэм Мендес', 2022, 16, 3.8),
  ('Фантастика', 'Джеймс Кэмерон', 2021, 52, 9.1),
  ('Боевик', 'Майкл Манн', 1995, 70, 7.7),
  ('Драма', 'Томас Винтерберг', 1987, 198, 9.3),
  ('Вестерн', 'Говард Хоукс', 1962, 23, 2.9),
  ('Мультфильм', 'Пит Доктер', 2016, 194, 8.1),  
  ('Комедия', 'Райан Джонсон', 2019, 131, 7.8),
  ('Триллер', 'Джон Карпентер', 1980, 92, 1),
  ('Фантастика', 'Ридли Скотт', 1984, 115, 8.5),
  ('Боевик', 'Пол Гринграсс', 1990, 33, 7.3),
  ('Драма', 'Педро Альмодовар', 2003, 104, 8.2),
  ('Драма', 'Фрэнсис Форд Коппола', 1972, 175, 10.1),
  ('Криминал', 'Квентин Тарантино', 1994, 54, 11.0),
  ('Боевик', 'Квентин Тарантино', 1999, 154, 8.0),
  ('Драма', 'Педро Альмодовар', 2007, 104, 8.2),
  ('Фантастика', 'Джеймс Кэмерон', 2001,122, 9.1),
  ('Драма', 'Сэм Мендес', 2019, 116, 9.8),
  ('Криминал', 'Квентин Тарантино', 2021, 144, 9.0)
  ;

SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_before_1990;
SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_1991_2000;
SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_2001_2010;
SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_2011_2020;
SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_after_2021;

SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_short;
SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_medium;
SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_long;
SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_extra_long;

SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_low_rated;
SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_medium_rated;
SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies_high_rated;

SELECT movie_type, director, year_of_issue, lenght_in_minutes, rate FROM movies;

