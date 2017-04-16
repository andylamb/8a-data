DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS Boulders;

CREATE TABLE Users (
  user_id int PRIMARY KEY,
  name text NOT NULL,
  height_string text,
  weight_string text,
  country text,
  city text,
  dob date,
  started_climbing int,
  occupation text,
  other_interests text,
  best_comp_result text,
  best_climbing_area text,
  guide_areas text,
  sponsor text,
  presentation_vists int NOT NULL,
  routes_vists int NOT NULL,
  boulders_vists int NOT NULL,
  blog_vists int NOT NULL,
  total_vists int NOT NULL,
  r_country_score int,
  r_country_ranking_string text,
  r_world_ranking_string text,
  r_all_time_country_score int,
  r_all_time_country_ranking_string text,
  r_all_time_world_ranking_string text,
  b_country_score int,
  b_country_ranking_string text,
  b_world_ranking_string text,
  b_all_time_country_score int,
  b_all_time_country_ranking_string text,
  b_all_time_world_ranking_string text
);

CREATE TABLE Boulders (
  user_id int,
  name text NOT NULL,
  grade text NOT NULL,
  date date NOT NULL,
  type text CHECK(type IN ('Flash', 'Onsight', 'Redpoint')) NOT NULL,
  recommend int CHECK(recommend in (0, 1)) NOT NULL,
  area text NOT NULL,
  tags text,
  comment text,
  stars int CHECK(stars in (0, 1, 2 ,3)) NOT NULL,
  FOREIGN KEY(user_id) REFERENCES Users(user_id)
);

CREATE TABLE ScrapeExceptions
(
    user_id INTEGER PRIMARY KEY NOT NULL,
    exception TEXT
);