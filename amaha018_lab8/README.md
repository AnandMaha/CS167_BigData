# Lab 8

## Student information
* Full name: Anand Mahadevan
* E-mail: amaha018@ucr.edu
* UCR NetID: amaha018
* Student ID: 862132182

## Answers

* (Q1) What is the result?
  ```text
  5, the month that this lab is being completed in (May).
  ```
  

* (Q2) Which query did you use and what is the answer?
  
  ```sql
  SELECT COUNT(DISTINCT(primary_type))
  FROM ChicagoCrimes
  WHERE location_description = "GAS STATION"
  ```

  ```text
  5
  ```

* (Q3) Include the query in your README file

  ```sql
  SELECT year, COUNT(*) as count
  FROM ChicagoCrimes
  GROUP BY year
  ORDER BY count DESC 
  ```

* (Q4) Which `district` has the most number of crimes? Include the query and the answer in the README file.

  ```sql
  SELECT district, COUNT(*) as count
  FROM ChicagoCrimes
  GROUP BY district
  ORDER BY count DESC
  LIMIT 1;
  ```

  ```text
  District 18
  ```

* (Q5) Include the query in your submission.

  ```sql
  SELECT year_name,month_number, count(*) AS count FROM (
  SELECT print_datetime(parse_datetime(date_value, "MM/DD/YYYY hh:mm:ss a"), "YYYY") AS year_name,
  get_month(parse_datetime(date_value, "MM/DD/YYYY hh:mm:ss a")) AS month_number
  FROM ChicagoCrimes
  ) AS asdf
  GROUP BY year_name,month_number
  ORDER BY year_name,month_number
  ```

* (Q6) What is the total number of results produced by this query (not only the shown ones)?
  - 232 results