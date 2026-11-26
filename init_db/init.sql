-- Виконується один раз під час першого запуску контейнера MySQL
CREATE DATABASE IF NOT EXISTS olympic_dataset;

CREATE TABLE olympic_dataset.athlete_event_results (
    edition VARCHAR(50),
    edition_id INT,
    country_noc VARCHAR(10),
    sport VARCHAR(50),
    event VARCHAR(200),
    result_id INT,
    athlete VARCHAR(100),
    athlete_id INT,
    pos VARCHAR(20),
    medal VARCHAR(10),
    isTeamSport VARCHAR(5)
) CHARACTER SET utf8mb4;

-- Порожнє значення medal у CSV означає, що медалі немає (NULL)
LOAD DATA INFILE '/var/lib/mysql-files/Olympic_Athlete_Event_Results.csv'
INTO TABLE olympic_dataset.athlete_event_results
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(edition, edition_id, country_noc, sport, event, result_id, athlete, athlete_id,
 pos, @medal, isTeamSport)
SET medal = NULLIF(@medal, '');
