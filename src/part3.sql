-- 1) Write a function that returns the TransferredPoints table in a more human-readable form
DROP FUNCTION IF EXISTS TransferredPoints_v2();

CREATE OR REPLACE FUNCTION TransferredPoints_v2()
RETURNS TABLE(Peer1 VARCHAR, Peer2 VARCHAR, "PointsAmount" BIGINT) AS $$
    BEGIN
	    RETURN QUERY
	    WITH revers AS (
		    SELECT
			    CASE WHEN checkingpeer > checkedpeer THEN checkingpeer ELSE checkedpeer END AS checkingpeer,
			    CASE WHEN checkingpeer > checkedPeer THEN checkedpeer ELSE checkingpeer END AS checkedpeer,
			    CASE WHEN checkingpeer > checkedPeer THEN pointsamount ELSE -pointsamount END AS pointsamount
		    FROM TransferredPoints)
	    SELECT checkingpeer AS Peer1, checkedpeer AS Peer2, SUM(pointsamount) AS PointsAmount
	    FROM revers
	    GROUP BY checkingpeer, checkedpeer;
    END;
$$ LANGUAGE plpgsql;

SELECT * FROM TransferredPoints_v2();

------------------------------------------------------------------------------------------------------------------------
-- 2) Write a function that returns a table of the following form: user name, name of the checked task, number of XP received
DROP FUNCTION IF EXISTS Success_Checks();

CREATE OR REPLACE FUNCTION Success_Checks()
RETURNS TABLE(Peer VARCHAR,  Task VARCHAR, XP INT) AS $$
    BEGIN
	    RETURN QUERY
	        SELECT t2.Peer AS Peer, t2.Task AS Task , t1.XPAmount AS XP
            FROM XP t1
            INNER JOIN Checks t2 ON t2.id = t1.Checkid;
    END;
$$ LANGUAGE plpgsql;

SELECT * FROM Success_Checks();

------------------------------------------------------------------------------------------------------------------------
-- 3) Write a function that finds the peers who have not left campus for the whole day
DROP FUNCTION IF EXISTS input_output(day date);

CREATE OR REPLACE FUNCTION input_output(IN day date)
RETURNS SETOF varchar AS $$
    BEGIN
        RETURN QUERY (SELECT peer
                      FROM timetracking
                      WHERE date = day
                      GROUP BY peer, date
                      HAVING COUNT(state) < 3);
    END;
$$ LANGUAGE plpgsql;

SELECT *
FROM input_output('12.05.2022');

------------------------------------------------------------------------------------------------------------------------
-- 4) Calculate the change in the number of peer points of each peer using the TransferredPoints table
DROP FUNCTION IF EXISTS modification_point();

CREATE OR REPLACE FUNCTION modification_point()
RETURNS TABLE(Peer VARCHAR, PointsChange NUMERIC) AS $$
    BEGIN
        RETURN QUERY
            WITH all_sum AS (
                    SELECT CheckingPeer AS Peer, SUM(PointsAmount) AS point
		            FROM TransferredPoints
		            GROUP BY CheckingPeer
                    UNION ALL
  		            SELECT CheckedPeer AS Peer, -SUM(PointsAmount) AS point
		            FROM TransferredPoints
		            GROUP BY CheckedPeer)
        SELECT all_sum.Peer , -SUM(all_sum.point)
        FROM all_sum
        GROUP BY all_sum.Peer
        ORDER BY 2 DESC;
    END;
$$ LANGUAGE plpgsql;

SELECT *
FROM modification_point();

------------------------------------------------------------------------------------------------------------------------
-- 5) Calculate the change in the number of peer points of each peer using the table returned by the first function from Part 3
DROP FUNCTION IF EXISTS modification_point_in_TransferredPoints_v2();

CREATE OR REPLACE FUNCTION modification_point_in_TransferredPoints_v2()
RETURNS TABLE(Peer VARCHAR, PointsChange NUMERIC) AS $$
    BEGIN
        RETURN QUERY
            WITH all_sum AS (
                    SELECT Peer1 AS Peer, SUM("PointsAmount") AS point
		            FROM TransferredPoints_v2()
		            GROUP BY Peer1
                    UNION ALL
  		            SELECT Peer2 AS Peer, -SUM("PointsAmount") AS point
		            FROM TransferredPoints_v2()
		            GROUP BY Peer2)
        SELECT all_sum.Peer , -SUM(all_sum.point)
        FROM all_sum
        GROUP BY all_sum.Peer
        ORDER BY 2 DESC;
    END;
$$ LANGUAGE plpgsql;

SELECT *
FROM modification_point_in_TransferredPoints_v2();

------------------------------------------------------------------------------------------------------------------------
-- 6) Find the most frequently checked task for each day
DROP FUNCTION IF EXISTS popular_task();

CREATE OR REPLACE FUNCTION popular_task()
RETURNS TABLE(Day DATE , "Task" VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT
        ranked."Date",
        ranked.Task
    FROM (
        SELECT
            Date AS "Date",
            Task AS Task,
            ROW_NUMBER() OVER (PARTITION BY Date ORDER BY COUNT(*) DESC) AS row_num
        FROM Checks
        GROUP BY Date, Task
    ) AS ranked
    WHERE row_num = 1;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM popular_task();

------------------------------------------------------------------------------------------------------------------------
-- 7) Find all peers who have completed the whole given block of tasks and the completion date of the last task
DROP PROCEDURE IF EXISTS CompleteBlockOfTasks(BlockTask VARCHAR, cursor refcursor) CASCADE;

CREATE OR REPLACE PROCEDURE CompleteBlockOfTasks (
    IN BlockTask VARCHAR,
    IN OUT cursor refcursor
) AS $$
BEGIN
    OPEN cursor FOR
-- WITH для создания временных таблиц task, last и success
    WITH task AS (
        --  выбираются все задачи, у которых заголовок начинается с BlockTask и за которыми следует цифра.
        SELECT * FROM Tasks
        WHERE title SIMILAR TO concat(BlockTask,'[0-9]%')
    ), last AS (
    --  выбирается максимальный заголовок из таблицы task
        SELECT MAX(title) AS title FROM task
    ), success AS (
    --  выбираются данные о проверках, которые имеют статус "Success" и соответствуют задачам из таблицы last.
        SELECT checks.peer, checks.task, checks.date FROM checks
        JOIN P2P ON checks.id = P2P.checkid
        WHERE P2P.state = 'Success'
        GROUP BY checks.peer, checks.task, checks.date
    )
    SELECT success.peer AS Peer, success.date AS Day FROM success
    JOIN last ON success.task = last.title;
END;
$$ LANGUAGE plpgsql;

-- вызов процедуры CompleteBlockOfTasks
BEGIN;
    CALL CompleteBlockOfTasks('C', 'cursor');
    FETCH ALL IN "cursor";
END;

------------------------------------------------------------------------------------------------------------------------
-- 8) Determine which peer each student should go to for a check.
DROP FUNCTION IF EXISTS BestPeer() CASCADE;

CREATE OR REPLACE FUNCTION BestPeer()
RETURNS TABLE (Peer VARCHAR, RecommendedPeer VARCHAR) AS $$
BEGIN
RETURN QUERY
    SELECT Cooked_table.Peer, Cooked_table.RecommendedPeer
    FROM (
        SELECT
            temp.Peer, temp.RecommendedPeer,
            RANK() OVER (PARTITION BY temp.Peer ORDER BY temp.Count_recommendations DESC) AS Rank_recommendations
        FROM (
            SELECT Friends.Peer1 AS Peer, Recommendations.RecommendedPeer, COUNT(*) AS Count_recommendations
            FROM Friends
            JOIN Recommendations ON Friends.Peer2 = Recommendations.Peer
            WHERE Friends.Peer1 <> Recommendations.RecommendedPeer
            GROUP BY Friends.Peer1, Recommendations.RecommendedPeer
            UNION
            SELECT Friends.Peer2 AS Peer, Recommendations.RecommendedPeer, COUNT(*) AS Count_recommendations
            FROM Friends
            JOIN Recommendations ON Friends.Peer1 = Recommendations.Peer
            WHERE Friends.Peer2 <> Recommendations.RecommendedPeer
            GROUP BY Friends.Peer2, Recommendations.RecommendedPeer
        ) AS temp
    ) AS Cooked_table
    where Rank_recommendations = 1;
END;
$$ LANGUAGE plpgsql;

select * from BestPeer();
select * from Friends;
select * from Recommendations;

------------------------------------------------------------------------------------------------------------------------
-- 9) Determine the percentage of peers who:
-- Started only block 1
-- Started only block 2
-- Started both
-- Have not started any of them
DROP FUNCTION IF EXISTS updatec();

CREATE OR REPLACE FUNCTION updatec()
RETURNS TABLE (StartedBlock_SQL INTEGER, StartedBlock_D INTEGER, StartedBothBlocks INTEGER, DidntStartAnyBlock INTEGER) AS $$
BEGIN
    RETURN QUERY
    WITH tmp AS (
        SELECT 
            (SELECT COUNT(Nickname) FROM Peers) as count_perrs,
            (SELECT COUNT(peer) FROM (SELECT peer FROM Checks WHERE task LIKE 'SQL%' GROUP BY peer) as ch) as count_perrs_block1,
            (SELECT COUNT(peer) FROM (SELECT peer FROM Checks WHERE task LIKE 'D%' GROUP BY peer) as ch) as count_perrs_block2,
            (SELECT COUNT(peer) FROM (SELECT peer FROM Checks WHERE task LIKE 'D%' OR task LIKE 'SQL%' GROUP BY peer) as ch) as count_perrs_block3,
            (SELECT COUNT(peer) FROM (SELECT peer FROM Checks WHERE task NOT LIKE 'D%' AND task NOT LIKE 'SQL%' GROUP BY peer) as ch) as count_perrs_block4
    )
    SELECT 
    CASE WHEN count_perrs = 0 THEN NULL ELSE ((count_perrs_block1::DECIMAL/count_perrs*100))::INTEGER END, 
    CASE WHEN count_perrs = 0 THEN NULL ELSE ((count_perrs_block2::DECIMAL/count_perrs*100))::INTEGER END,
    CASE WHEN count_perrs = 0 THEN NULL ELSE ((count_perrs_block3::DECIMAL/count_perrs*100))::INTEGER END,
    CASE WHEN count_perrs = 0 THEN NULL ELSE ((count_perrs_block4::DECIMAL/count_perrs*100))::INTEGER END
    FROM tmp;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM updatec();

------------------------------------------------------------------------------------------------------------------------
-- 10) Determine the percentage of peers who have ever successfully passed a check on their birthday
DROP FUNCTION IF EXISTS upd() CASCADE;

CREATE OR REPLACE FUNCTION upd()
RETURNS TABLE (SuccessfulChecks INTEGER, UnsuccessfulChecks INTEGER) AS $$
BEGIN
    RETURN QUERY
    WITH tmp AS (
            SELECT
            COUNT(tp_success.id) as SuccessBirt,
            COUNT(tp_failure.id) as FailBirt,
            COUNT(tp_all.id) as al
            FROM Peers p
            LEFT JOIN Checks c ON extract(month FROM p.Birthday) = extract(month FROM c.date)
                                      AND extract(day FROM p.Birthday) = extract(day FROM c.date) AND c.peer = p.Nickname
            LEFT JOIN P2P tp_success ON tp_success.checkid = c.id AND tp_success.state = 'Success'
            LEFT JOIN P2P tp_failure ON tp_failure.checkid = c.id AND tp_failure.state = 'Failure'
            LEFT JOIN P2P tp_all ON tp_all.checkid = c.id
            WHERE tp_all.state != 'Start'
        )
SELECT 
    CASE WHEN al = 0 THEN NULL ELSE ((SuccessBirt::DECIMAL/al*100))::INTEGER END AS SuccessfulChecks, 
    CASE WHEN al = 0 THEN NULL ELSE ((FailBirt::DECIMAL/al*100))::INTEGER END AS UnsuccessfulChecks
     FROM tmp;
END;
$$ LANGUAGE plpgsql;
    
SELECT * FROM upd();

------------------------------------------------------------------------------------------------------------------------
-- 11) Determine all peers who did the given tasks 1 and 2, but did not do task 3
DROP FUNCTION IF EXISTS finished_tasks(task1 VARCHAR, task2 VARCHAR, task3 VARCHAR) CASCADE;

CREATE OR REPLACE FUNCTION finished_tasks(
    task1 VARCHAR, 
    task2 VARCHAR,
    task3 VARCHAR
)
RETURNS TABLE (peer VARCHAR) AS $$
BEGIN
    RETURN QUERY
    WITH task_1_2 AS (
        SELECT temp.peer, count(temp.peer)
        FROM (
            (SELECT * FROM Checks 
             JOIN XP  ON Checks.id = XP.checkid WHERE task = task1)
            UNION
            (SELECT * FROM Checks 
             JOIN XP  ON Checks.id = XP.checkid WHERE task = task2)
        ) AS temp
        GROUP BY temp.peer
        HAVING count(temp.peer) = 2
    )
    SELECT task_1_2.peer FROM task_1_2
    EXCEPT
    SELECT task_1_2.peer FROM task_1_2 
    JOIN Checks  ON Checks.peer = task_1_2.peer
    JOIN XP  ON Checks.id = XP.checkid WHERE task = task3;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM finished_tasks('C2', 'C3','C4');

------------------------------------------------------------------------------------------------------------------------
-- 12) Using recursive common table expression, output the number of preceding tasks for each task
DROP FUNCTION IF EXISTS recurs(namem VARCHAR);

CREATE OR REPLACE FUNCTION recurs(namem VARCHAR)
RETURNS TABLE (num INTEGER) AS $$
BEGIN
    RETURN QUERY
    WITH RECURSIVE tree (title,parenttask, num)
    AS (
        SELECT t1.title, t1.parenttask, 0 AS num FROM Tasks t1
        WHERE t1.title = namem 
        UNION ALL
        SELECT t1.title, t1.parenttask, t.num + 1 AS num
        FROM Tasks t1
        JOIN tree t ON t1.title = t.parenttask
        WHERE t1.title IS NOT NULL
    )
    SELECT MAX(t.num::INTEGER) FROM tree AS t;
END;
$$ LANGUAGE plpgsql;

SELECT t.title as Task, recurs(t.title) as PrevCount FROM Tasks t;

------------------------------------------------------------------------------------------------------------------------
-- 13) Find "lucky" days for checks. A day is considered "lucky" if it has at least N consecutive successful checks
DROP PROCEDURE IF EXISTS LuckyDays CASCADE;

CREATE OR REPLACE PROCEDURE LuckyDays (
    IN num INT, 
    IN cursor refcursor default 'cursor'
) AS $$
BEGIN
    OPEN cursor FOR 
    WITH lucky AS (
        SELECT * FROM Checks
        JOIN P2P ON P2P.checkid = Checks.id
        JOIN Verter ON Verter.checkid = Checks.id
        JOIN Tasks ON Tasks.title = Checks.task
        JOIN XP ON XP.checkid = Checks.id
        WHERE P2P.state = 'Success' AND Verter.state = 'Success'
    )
        SELECT date FROM lucky
        WHERE lucky.xpamount >= lucky.maxxp * 0.8
        GROUP BY date
        -- команда используется в запросах с группировкой (GROUP BY) и фильтрует результаты группировки по условию,
        HAVING COUNT(date) >= num;
END;
$$ LANGUAGE plpgsql;

-- вызов процедуры LuckyDays
BEGIN;
CALL LuckyDays(1);
FETCH ALL IN "cursor";
END;

------------------------------------------------------------------------------------------------------------------------
-- 14) Find the peer with the highest amount of XP
DROP FUNCTION IF EXISTS MAX_experience CASCADE;

CREATE OR REPLACE FUNCTION MAX_experience()
RETURNS TABLE (Peer VARCHAR, XP BIGINT) AS $$
BEGIN
	RETURN QUERY
    SELECT Checks.Peer AS Peer, SUM(XP.XPAmount) AS XP FROM Checks 
    JOIN XP ON XP.checkid = Checks.id
    GROUP BY Checks.Peer
    ORDER BY XP DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM MAX_experience();

------------------------------------------------------------------------------------------------------------------------
-- 15) Determine the peers that came before the given time at least N times during the whole time
DROP PROCEDURE IF EXISTS early_peers CASCADE;

CREATE OR REPLACE PROCEDURE early_peers(
  IN early_time TIME,
  IN early_quantity INTEGER,
  IN OUT cursor refcursor
) AS $$
BEGIN
  OPEN cursor FOR
    SELECT TimeTracking.Peer FROM TimeTracking 
    WHERE TimeTracking.time < early_time
    GROUP BY TimeTracking.Peer
    HAVING COUNT(TimeTracking.Peer) >= early_quantity;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL early_peers('13:00:00', 1,'cursor');
FETCH ALL FROM "cursor";
END;

------------------------------------------------------------------------------------------------------------------------
-- 16) Determine the peers who left the campus more than M times during the last N days
DROP PROCEDURE IF EXISTS leaving_peers CASCADE;

CREATE OR REPLACE PROCEDURE leaving_peers(
  IN days INTEGER,
  IN leaving_quantity INTEGER,
  IN OUT cursor refcursor
) AS $$
BEGIN
    OPEN cursor FOR
        SELECT TimeTracking.Peer FROM TimeTracking
        WHERE TimeTracking.state = '2' AND date >= (now() - (days - 1 || ' days')::INTERVAL)::DATE
                AND date <= now()::DATE
        GROUP BY TimeTracking.Peer
        HAVING COUNT(TimeTracking.state) >= leaving_quantity;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL leaving_peers(356, 2,'cursor');
FETCH ALL FROM "cursor";
END;

------------------------------------------------------------------------------------------------------------------------
-- 17) Determine for each month the percentage of early entries
DROP FUNCTION IF EXISTS month_birthday() CASCADE;

CREATE OR REPLACE FUNCTION month_birthday()
RETURNS TABLE (Month VARCHAR, EarlyEntries INTEGER) AS $$
BEGIN
    RETURN QUERY
    WITH tmp as (SELECT 
    TO_CHAR(Peers.birthday, 'Month') AS Mont,
    ROUND(CAST(SUM(CASE WHEN EXTRACT(HOUR FROM TimeTracking.time) < 12 THEN 1 ELSE 0 END) AS NUMERIC)/ SUM(1) * 100) AS EarlyEntrie
    FROM Peers
    JOIN TimeTracking ON TimeTracking.peer = Peers.Nickname
    GROUP BY Mont
    ORDER BY MIN(EXTRACT(MONTH FROM Peers.birthday)))
SELECT Mont::VARCHAR, EarlyEntrie::INTEGER FROM tmp
WHERE EarlyEntrie <> 0;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM month_birthday()