/* job_data*/
CREATE TABLE job_data (
  ds DATE,
  job_id INTEGER NOT NULL,
  actor_id INTEGER NOT NULL,
  event VARCHAR(20) NOT NULL,
  language VARCHAR(20) NOT NULL,
  time_spent INTEGER NOT NULL,
  org VARCHAR(20) NOT NULL );


INSERT INTO job_data (ds, job_id, actor_id, event, language, time_spent, org)
VALUES ('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'),
    ('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
    ('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
    ('2020-11-28', 23, 1005,'transfer', 'Persian', 22, 'D'),
    ('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
    ('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
    ('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
    ('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');


/* users */
CREATE TABLE users (
    user_id	INT primary key,
    created_at timestamp,
    company_id INT,
    language VARCHAR(512),
    activated_at timestamp,
    state VARCHAR(512)
);

COPY users 
FROM 'C:\Program Files\PostgreSQL\16\data\Case Study 2\users.csv'
DELIMITER ','
CSV HEADER;


/* events */
CREATE TABLE events (
    user_id INTEGER,
    occurred_at timestamp,
    event_type VARCHAR(50),
    event_name VARCHAR(50),
    location VARCHAR(50),
    device VARCHAR(50),
    user_type INTEGER
);

COPY events 
FROM 'C:\Program Files\PostgreSQL\16\data\Case Study 2\events.csv'
DELIMITER ','
CSV HEADER;


/* email_events */
CREATE TABLE email_events (
    user_id INT,
    occurred_at timestamp,
    action VARCHAR(512),
    user_type int
);

COPY email_events 
FROM 'C:\Program Files\PostgreSQL\16\data\Case Study 2\email_events.csv'
DELIMITER ','
CSV HEADER;


-- Case study 1 : Job Data Analysis
-- Jobs Reviewed Over Time
-- Objective: Calculate the number of jobs reviewed per hour for each day in November 2020.
-- Task: Write an SQL query to calculate the number of jobs reviewed per hour for each day in November 2020.
WITH daily_jobs AS (
  SELECT ds, (COUNT(job_id) * 3600 / SUM(time_spent)) AS jobs_per_hour
  FROM job_data
  WHERE ds BETWEEN '2020-11-01' AND '2020-12-01'
  GROUP BY ds)
	
SELECT 
  ROUND(AVG(jobs_per_hour), 2) AS "avg_jobs_reviewed_per_hour"
FROM 
  daily_jobs;


-- Throughput Analysis
-- Objective: Calculate the 7-day rolling average of throughput (number of events per second).
-- Task: Write an SQL query to calculate the 7-day rolling average of throughput.
WITH daily_throughput AS (SELECT ds, 
	(COUNT(job_id)::decimal / SUM(time_spent)) AS throughput_per_second
  	FROM job_data
  	GROUP BY ds
),
rolling_avg_throughput AS (SELECT ds,
	AVG(throughput_per_second) OVER (ORDER BY ds 
						ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_throughput
  	FROM daily_throughput
)

SELECT ds, ROUND(rolling_avg_throughput, 4) AS "7_day_rolling_avg_throughput"
FROM rolling_avg_throughput;


-- Language Share Analysis
-- Objective: Calculate the percentage share of each language in the last 30 days.
-- Task: Write an SQL query to calculate the percentage share of each language over the last 30 days.
WITH total_count AS ( SELECT language, COUNT(*) AS total
	FROM job_Data
	GROUP BY language)

SELECT language, total,
  ROUND((total / SUM(total) OVER ()) * 100, 2) AS percentage
FROM total_count
ORDER BY percentage DESC;


-- Duplicate row detection
-- Objective: Identify duplicate rows in the data.
-- Task: Write an SQL query to display duplicate rows from the job_data table.

-- Duplicate records based on all column -> o/p : no records
SELECT *
FROM job_data
GROUP BY job_id, actor_id, event, language, time_spent, org, ds
HAVING COUNT(*) > 1;

-- Duplicate records based on only job_id -> o/p : 1 job_id with 3 records
SELECT job_id, COUNT(job_id) AS total_records
FROM job_data
GROUP BY job_id
HAVING COUNT(job_id) > 1;


-- Case study 2 : Investigating Metric Spike
-- Weekly User Engagement
-- Objective: Measure the activeness of users on a weekly basis.
-- Task: Write an SQL query to calculate the weekly user engagement.
SELECT EXTRACT(WEEK FROM occurred_at) AS week,
  COUNT(DISTINCT user_id) AS active_users,
  COUNT(event_type) AS total_events
FROM events
WHERE event_type = 'engagement'
GROUP BY week
ORDER BY week;


-- User Growth Analysis
-- Objective: Analyze the growth of users over time for a product.
-- Task: Write an SQL query to calculate the user growth for the product.

-- Top 20 sample data output
SELECT EXTRACT(MONTH FROM created_at) AS month,
	EXTRACT(WEEK FROM created_at) AS week,
  	COUNT(user_id) AS new_users_weekly,
	SUM(COUNT(user_id)) OVER(PARTITION BY EXTRACT(MONTH FROM created_at)) AS new_users_monthly
FROM users
GROUP BY 1, 2
ORDER BY 1, 2
LIMIT 20;


-- Weekly Retention Analysis
-- Objective: Analyze the retention of users on a weekly basis after signing up for a product.
-- Task: Write an SQL query to calculate the weekly retention of users based on their sign-up cohort.
WITH user_signup AS (SELECT user_id, EXTRACT(WEEK FROM occurred_at) AS signup_week
	FROM events 
	WHERE event_type = 'signup_flow' AND event_name = 'complete_signup'
	),
	user_engagement AS (SELECT user_id, 
		EXTRACT(WEEK FROM occurred_at) AS engagement_week
	FROM events  WHERE event_type = 'engagement'
	)
SELECT signup_week AS week_num, COUNT(CASE WHEN e.engagement_week - 	s.signup_week = 1 THEN 1 END) AS ‘users_retained’
FROM user_signup s
JOIN user_engagement e ON s.user_id = e.user_id
GROUP BY week_num 
ORDER BY week_num;


-- Weekly Engagement per Device
-- Objective: Measure the activeness of users on a weekly basis per device.
-- Task: Write an SQL query to calculate the weekly engagement per device.
-- Top 10 sample data output

SELECT EXTRACT(WEEK FROM occurred_at) AS week, device, COUNT(DISTINCT user_id) AS total_users
FROM events 
WHERE event_type = 'engagement'	
GROUP BY week, device
ORDER BY week, device
LIMIT 10;

WITH t1 AS (SELECT device, EXTRACT(WEEK FROM occurred_at) AS week, COUNT(user_id) AS total_users
	FROM events 
	WHERE event_type = 'engagement'	
	GROUP BY device, week
	ORDER BY week, device
	)
SELECT device, ROUND(AVG(total_users)) AS weekly_engagement
FROM t1
GROUP BY device;


-- Email Engagement
-- Objective: Analyze how users are engaging with the email service.
-- Task: Write an SQL query to calculate the email engagement metrics.
WITH email AS (SELECT action, EXTRACT(WEEK FROM occurred_at) AS week,
	COUNT(user_id) AS total_users
	FROM email_events
	GROUP BY action, week)
SELECT action, ROUND(AVG(total_users)) AS avg_weekly_email_eng
FROM email
GROUP BY action;


SELECT EXTRACT(WEEK FROM occurred_at) AS week,
  	COUNT(CASE WHEN action = 'email_clickthrough' THEN 1 END) AS email_clickthrough,
  	COUNT(CASE WHEN action = 'email_open' THEN 1 END) AS email_open,
  	COUNT(CASE WHEN action = 'sent_reengagement_email' THEN 1 END) AS sent_reengagement_email,
  	COUNT(CASE WHEN action = 'sent_weekly_digest' THEN 1 END) AS sent_weekly_digest
FROM email_events
GROUP BY week
ORDER BY week;




