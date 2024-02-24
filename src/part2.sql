-- 1) Write a procedure for adding P2P check
DROP PROCEDURE IF EXISTS p2p_check CASCADE;
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE p2p_check
    (IN checked_peer varchar, IN checking_peer varchar, IN task_name varchar, IN status check_status, IN check_time time) AS $$
DECLARE
    check_id bigint;
    last_status varchar;
BEGIN
    SELECT state INTO last_status FROM p2p WHERE checkingpeer = checking_peer ORDER BY id DESC LIMIT 1;
    IF status = 'Start' THEN
        IF last_status = 'Start' THEN RAISE EXCEPTION 'The checking of % is not finished', checked_peer; END IF;
        SELECT (MAX(id)+1) INTO check_id FROM checks;
        INSERT INTO checks (id, peer, task, date)
        VALUES (check_id, checked_peer, task_name, current_date);
    ELSE
        IF last_status <> 'Start' THEN RAISE EXCEPTION '% did not start the project', checked_peer; END IF;
        SELECT checkid INTO check_id FROM p2p
        WHERE checking_peer = checkingpeer AND state = 'Start' ORDER BY time DESC LIMIT 1;
    END IF;
    INSERT INTO p2p (id, checkid, checkingpeer, state, time)
    VALUES ((SELECT(MAX(id)+1) FROM p2p), check_id, checking_peer, status, check_time);
END;
$$ LANGUAGE plpgsql;

-- call for start
-- CALL p2p_check('vrbyeonaxg', 'kdvfscrdbf', 'DO3', 'Start', '14:30:00');
--
-- -- call for result
-- CALL p2p_check('vrbyeonaxg', 'kdvfscrdbf', 'DO3', 'Failure', '14:45:00');
-- CALL p2p_check('vrbyeonaxg', 'kdvfscrdbf', 'DO3', 'Success', '14:45:00');
--
-- delete from p2p where checkid = 13932;
-- delete from checks where id = 13932;
-- select * from checks;
-- select * from p2p;

------------------------------------------------------------------------------------------------------------------------
-- 2) Write a procedure for adding checking by Verter
DROP PROCEDURE IF EXISTS verter_check CASCADE;
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE verter_check
    (IN checked_peer varchar, IN task_name varchar, IN status check_status, IN check_time time) AS $$
DECLARE
    check_id bigint;
    last_status varchar;
BEGIN
    SELECT state INTO last_status FROM verter ORDER BY id DESC LIMIT 1;
    IF status = 'Start' THEN
        IF last_status = 'Start' THEN
            RAISE EXCEPTION 'The checking of % is not finished', checked_peer;
        END IF;
        SELECT checkid INTO check_id FROM (SELECT * FROM p2p WHERE state = 'Success') AS sc
        JOIN checks ch on ch.id = sc.checkid
        WHERE ch.peer = checked_peer AND ch.task = task_name
        ORDER BY time DESC LIMIT 1;
        IF check_id IS NULL THEN
            RAISE EXCEPTION 'There is no %''s successful projects for verter to check', checked_peer;
        END IF;
        IF task_name NOT IN ('C1', 'C2', 'C3', 'C4', 'C5', 'C6') THEN
            RAISE EXCEPTION 'Invalid task type for verter check: %', task_name;
        END IF;
    ELSE
        IF last_status <> 'Start' THEN
            RAISE EXCEPTION 'There is no started verter for %''s projects', checked_peer;
        END IF;
        SELECT checkid INTO check_id FROM verter ORDER BY id DESC LIMIT 1;
    END IF;
    INSERT INTO verter(id, checkid, state, time)
    VALUES ((SELECT (MAX(id)+1) FROM verter), check_id, status, check_time);
END;
$$ LANGUAGE plpgsql;

-- -- call for start
-- CALL verter_check('vrbyeonaxg', 'DO3', 'Start', '14:50:00');
--
-- -- call for result
-- CALL verter_check('vrbyeonaxg', 'DO3', 'Success', '15:15:00');
-- CALL verter_check('vrbyeonaxg', 'DO3', 'Failure', '15:15:00');
--
-- select * from verter;
-- delete from verter where id = 10001;

------------------------------------------------------------------------------------------------------------------------
-- 3) Write a trigger: after adding a record with the "start" status to the P2P table,
-- change the corresponding record in the TransferredPoints table
DROP FUNCTION IF EXISTS upd_points CASCADE;
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION upd_points()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.State = 'Start' THEN
        INSERT INTO transferredpoints (id, checkingpeer, checkedpeer, pointsamount)
        VALUES ((SELECT (MAX(id)+1) FROM transferredpoints), NEW.checkingpeer,
                (SELECT peer FROM checks WHERE id = NEW.checkid), 1)
        ON CONFLICT (checkingpeer, checkedpeer)
            DO UPDATE SET pointsamount = transferredpoints.pointsamount + excluded.pointsamount;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tg_upd_points
AFTER INSERT ON p2p FOR EACH ROW
EXECUTE FUNCTION upd_points();

-- -- there is such string
-- CALL p2p_check('vrbyeonaxg', 'cpwfiewvim', 'DO3', 'Start', '14:30:00');
-- -- there is not such string
-- CALL p2p_check('vrbyeonaxg', 'kdvfscrdbf', 'DO3', 'Start', '14:30:00');
--
-- select * from transferredpoints where checkedpeer = 'vrbyeonaxg';
-- select * from transferredpoints where checkingpeer = 'cpwfiewvim' and checkedpeer = 'vrbyeonaxg';  -- there is such string
-- select * from transferredpoints where checkingpeer = 'kdvfscrdbf' and checkedpeer = 'vrbyeonaxg';  -- there is not such string
--
-- update transferredpoints set pointsamount = 1 where id = 9406;
-- delete from transferredpoints where id = 9407;

------------------------------------------------------------------------------------------------------------------------
-- 4) Write a trigger: before adding a record to the XP table, check if it is correct
DROP FUNCTION IF EXISTS check_xp CASCADE;
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION check_xp()
RETURNS TRIGGER AS $$
DECLARE
    max_xp INT := 0;
    task_type VARCHAR;
BEGIN
    SELECT maxxp, title INTO max_xp, task_type FROM tasks
    JOIN checks ch ON tasks.title = ch.task
    WHERE ch.ID = NEW.checkid;

    IF new.xpamount > max_xp THEN
        RAISE EXCEPTION 'Given xp amount of % is bigger than maximum of %', new.xpamount, max_xp;
    END IF;

    IF task_type IN ('C1', 'C2', 'C3', 'C4', 'C5', 'C6') THEN
        IF NOT EXISTS (SELECT 1 FROM verter WHERE CheckID = NEW.checkid AND State = 'Success') THEN
            RAISE EXCEPTION 'Verter check must be successful for tasks C1-C6.';
        END IF;
    ELSE
        IF NOT EXISTS (SELECT 1 FROM p2p WHERE CheckID = NEW.checkid AND State = 'Success') THEN
            RAISE EXCEPTION 'P2P checks must be successful for other tasks.';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tg_check_xp
BEFORE INSERT ON xp FOR EACH ROW
EXECUTE FUNCTION check_xp();

-- select * from xp;
-- select * from checks;
-- select * from tasks;
-- select * from p2p;
-- select * from verter;
--
-- INSERT INTO xp (id, checkid, xpamount) VALUES ((SELECT (MAX(id)+1) FROM xp), 13931, 260);
-- delete from xp where id = 2451;
--
-- INSERT INTO xp (id, checkid, xpamount) VALUES ((SELECT (MAX(id)+1) FROM xp), 13931, 777);
