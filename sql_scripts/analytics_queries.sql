-- Analytical queries written for data exploration as well as for dashboard creation


 -- Countries where users come from

SELECT country AS Country,
       COUNT(user_id) AS "Number of users"
FROM user_schema.geo AS g
WHERE country != 'UNKNOWN'
GROUP BY country
ORDER BY "Number of users" DESC;


-- Gender distribution

SELECT gender,
       COUNT(gender) AS number_per_gender
FROM user_schema.user
GROUP BY gender;


-- Number of users

SELECT COUNT(pid)
FROM user_schema.user;


-- Average price per working hour (whole sample)

SELECT ROUND(CAST(AVG(e.price_per_hour) AS numeric), 2) AS "Average price per hour"
FROM user_schema.earnings AS e;


-- Average earnings (in thousands of $, whole sample)

SELECT ROUND(CAST(AVG(e.earnings_in_thousands) AS numeric), 2) AS "Average earnings in thousands of $"
FROM user_schema.earnings AS e;


-- Average job success (for users where this information is available, whole sample)

SELECT ROUND(CAST(AVG(j.job_success_perc) AS numeric), 2) AS "Average job success"
FROM user_schema.jobs AS j
WHERE j.job_success_perc != '0';


-- Average earnings (in thousands of $, and average price per hour in $)

SELECT g.country AS "Country",
       ROUND(AVG(e.earnings_in_thousands), 2) AS "Average earnings in thousands of $",
       ROUND(AVG(e.price_per_hour), 2) AS "Average price per hour in $"
FROM user_schema.user AS f
INNER JOIN user_schema.earnings AS e ON f.pid = e.pid
INNER JOIN user_schema.geo AS g ON e.pid = g.pid
GROUP BY g.country
ORDER BY "Average earnings in thousands of $" DESC;


-- Distribution of users per gender and country

SELECT f.gender,
       COUNT(f.gender) AS "Number per gender",
       g.country
FROM user_schema.user AS f
INNER JOIN user_schema.geo AS g ON f.pid = g.pid
INNER JOIN user_schema.earnings AS e ON f.pid = e.pid
GROUP BY f.gender,
         g.country
ORDER BY "Number per gender" DESC;


-- Number of users per profession

 WITH profession_cte AS
  (SELECT j.main_profession,
          CASE j.main_profession
              WHEN '1.0' THEN 'Clerical and data entry'
              WHEN '2.0' THEN 'Creative and multimedia'
              WHEN '3.0' THEN 'Professional services'
              WHEN '4.0' THEN 'Sales and marketing support'
              WHEN '5.0' THEN 'Software dev and tech'
              WHEN '6.0' THEN 'Writing and translation'
              ELSE 'NOT DEFINED'
          END AS profession_class
   FROM user_schema.jobs AS j)
SELECT profession_class AS "profession class",
       COUNT(main_profession) AS "Number of users per profession"
FROM profession_cte
GROUP BY "profession class"
ORDER BY "Number of users per profession" DESC;


-- Average price per hour ($) per profession class

 WITH profession_cte AS
  (SELECT j.main_profession,
          j.pid,
          CASE j.main_profession
              WHEN '1.0' THEN 'Clerical and data entry'
              WHEN '2.0' THEN 'Creative and multimedia'
              WHEN '3.0' THEN 'Professional services'
              WHEN '4.0' THEN 'Sales and marketing support'
              WHEN '5.0' THEN 'Software dev and tech'
              WHEN '6.0' THEN 'Writing and translation'
              ELSE 'NOT DEFINED'
          END AS profession_class
   FROM user_schema.jobs AS j)
SELECT profession_class AS "profession class",
       ROUND(CAST(AVG(e.price_per_hour) AS numeric), 2) AS "Average price per hour"
FROM profession_cte AS oc
INNER JOIN user_schema.earnings AS e ON oc.pid = e.pid
GROUP BY "profession class"
ORDER BY "Average price per hour" DESC;


-- Share of professions, per country

 WITH profession_cte AS
  (SELECT j.main_profession,
          j.pid,
          CASE j.main_profession
              WHEN '1.0' THEN 'Clerical and data entry'
              WHEN '2.0' THEN 'Creative and multimedia'
              WHEN '3.0' THEN 'Professional services'
              WHEN '4.0' THEN 'Sales and marketing support'
              WHEN '5.0' THEN 'Software dev and tech'
              WHEN '6.0' THEN 'Writing and translation'
              ELSE 'NOT DEFINED'
          END AS profession_class
   FROM user_schema.jobs AS j)
SELECT pcte.profession_class,
       g.country,
       COUNT(pcte.main_profession) AS profession_count
FROM profession_cte AS pcte
INNER JOIN user_schema.geo AS g ON pcte.pid = g.pid
GROUP BY pcte.profession_class,
         g.country
ORDER BY g.country,
         profession_count DESC;

        
-- Price per hour ($) for each professions, per country

 WITH profession_cte AS
  (SELECT j.main_profession,
          j.pid,
          CASE j.main_profession
              WHEN '1.0' THEN 'Clerical and data entry'
              WHEN '2.0' THEN 'Creative and multimedia'
              WHEN '3.0' THEN 'Professional services'
              WHEN '4.0' THEN 'Sales and marketing support'
              WHEN '5.0' THEN 'Software dev and tech'
              WHEN '6.0' THEN 'Writing and translation'
              ELSE 'NOT DEFINED'
          END AS profession_class
   FROM user_schema.jobs AS j)
SELECT pcte.profession_class,
       g.country,
       AVG(e.price_per_hour) AS average_price
FROM profession_cte AS pcte
INNER JOIN user_schema.geo AS g ON pcte.pid = g.pid
INNER JOIN user_schema.earnings AS e ON pcte.pid = e.pid
GROUP BY pcte.profession_class,
         g.country
ORDER BY g.country DESC,
         average_price DESC;
        

-- professions of top 5 most paid users (earning in thousands $), per country

 WITH profession_cte AS
  (SELECT j.main_profession,
          j.pid,
          CASE j.main_profession
              WHEN '1.0' THEN 'Clerical and data entry'
              WHEN '2.0' THEN 'Creative and multimedia'
              WHEN '3.0' THEN 'Professional services'
              WHEN '4.0' THEN 'Sales and marketing support'
              WHEN '5.0' THEN 'Software dev and tech'
              WHEN '6.0' THEN 'Writing and translation'
              ELSE 'NOT DEFINED'
          END AS profession_class
   FROM user_schema.jobs AS j),
      ranked_users AS
  (SELECT pcte.profession_class,
          g.country,
          e.earnings_in_thousands,
          ROW_NUMBER() OVER (PARTITION BY g.country
                             ORDER BY e.earnings_in_thousands DESC) AS rank
   FROM profession_cte AS pcte
   INNER JOIN user_schema.geo AS g ON pcte.pid = g.pid
   INNER JOIN user_schema.earnings AS e ON pcte.pid = e.pid)
SELECT profession_class,
       country,
       earnings_in_thousands
FROM ranked_users
WHERE rank <= 5
ORDER BY country DESC,
         rank ASC;