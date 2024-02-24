DROP DATABASE IF EXISTS part4_info;

CREATE DATABASE part4_info;

DROP PROCEDURE IF EXISTS DropTable CASCADE;
DROP FUNCTION IF EXISTS list_of_scalar_functions() CASCADE;
DROP FUNCTION IF EXISTS test_function() CASCADE;
DROP PROCEDURE IF EXISTS GetScalarFunctions() CASCADE;
DROP PROCEDURE IF EXISTS description_functiobs() CASCADE;
DROP PROCEDURE IF EXISTS DropTrigger(IN tablename VARCHAR) CASCADE;

---------------------------------------------- for task 3 --------------------------------------------------------------
-- Создаем тестовую таблицу
CREATE TABLE test_table (
  id SERIAL PRIMARY KEY,
  name VARCHAR(50)
);

CREATE TABLE tmp_table();

-- Создаем функцию, которую вызывает триггер
CREATE FUNCTION test_function()
RETURNS TRIGGER AS $$
BEGIN
  -- some logic
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Создаем триггер для тестовой таблицы
CREATE TRIGGER test_trigger
AFTER INSERT ON test_table
FOR EACH ROW
EXECUTE FUNCTION test_function();
----------------------------------------- for task 4 -------------------------------------------------------------------
DROP FUNCTION IF EXISTS simple_function();
DROP FUNCTION IF EXISTS simple_function_2();
DROP FUNCTION IF EXISTS add_numbers(a INTEGER, b INTEGER);
DROP FUNCTION IF EXISTS greet(name VARCHAR);
----------
CREATE OR REPLACE FUNCTION simple_function()
RETURNS INTEGER AS $$
DECLARE
    result INTEGER;
BEGIN
    -- some logic
    RETURN result;
END;
$$ LANGUAGE plpgsql;
----------
CREATE OR REPLACE FUNCTION simple_function_2()
RETURNS INTEGER AS $$
DECLARE
    result INTEGER;
BEGIN
    -- some logic
    RETURN result;
END;
$$ LANGUAGE plpgsql;
----------
CREATE OR REPLACE FUNCTION add_numbers(a INTEGER, b INTEGER)
RETURNS INTEGER AS $$
BEGIN
    RETURN a + b;
END;
$$ LANGUAGE plpgsql;
----------
CREATE OR REPLACE FUNCTION greet(name VARCHAR DEFAULT 'World')
RETURNS VARCHAR AS $$
BEGIN
    RETURN 'Hello, ' || name || '!';
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------------------------------------------------------------------
-- 1) Create a stored procedure that, without destroying the database,
-- destroys all those tables in the current database whose names begin with the phrase 'TableName'.

CREATE OR REPLACE PROCEDURE DropTable(IN tablename VARCHAR) AS $$
DECLARE
    TableToDrop VARCHAR;
BEGIN
-- Цикл FOR выполняется для каждой таблицы, полученной из запроса SELECT, который выбирает table_name из information_schema.tables
-- с условием WHERE сначала строит tablename а потом символы любые '%'
    FOR TableToDrop IN SELECT table_name FROM information_schema.tables
        WHERE table_name LIKE concat(tablename, '%') AND table_schema = 'public'
-- Цикл LOOP пока не удалятся все таблицы с таким названием
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || TableToDrop || ' CASCADE';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL DropTable('t');
SELECT * FROM tmp_table;

------------------------------------------------------------------------------------------------------------------------
-- 2) Create a stored procedure with an output parameter
-- that outputs a list of names and parameters of all scalar user's SQL functions in the current database.

CREATE OR REPLACE FUNCTION list_of_scalar_functions()
RETURNS TABLE (FuncList VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT  array_agg(p.proname || '(' || pg_get_function_arguments(p.oid) || ')') ::VARCHAR AS FuncList
        FROM pg_proc p
        JOIN pg_namespace n on p.pronamespace = n.oid
        WHERE p.prokind = 'f'
          AND n.nspname = 'public'
          AND (pg_get_function_arguments(p.oid) != '')
        GROUP BY p.oid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE GetScalarFunctions(OUT FunctionCount INTEGER, OUT FunctionList TEXT[]) AS $$
BEGIN
    SELECT COUNT(*) INTO FunctionCount FROM list_of_scalar_functions();
    SELECT array_agg(FuncList) INTO FunctionList FROM list_of_scalar_functions();
END;
$$ LANGUAGE plpgsql;

CALL GetScalarFunctions(FunctionCount := NULL, FunctionList := NULL);

------------------------------------------------------------------------------------------------------------------------
-- 3) Create a stored procedure with output parameter, which destroys all SQL DML triggers in the current database.
-- The output parameter returns the number of destroyed triggers.
CREATE OR REPLACE PROCEDURE DropTrigger(IN tablename VARCHAR) AS
$$
DECLARE
    TriggerToDrop VARCHAR;
    DeletedTriggersCount INTEGER := 0;
BEGIN
     -- Цикл FOR выполняется для каждой таблицы, полученной из запроса SELECT, который выбирает table_name из information_schema.tables 
     FOR TriggerToDrop IN SELECT trigger_name  FROM information_schema.triggers 
     -- event_object_table - это столбец в таблице information_schema.triggers, который содержит имя таблицы, для которой создан триггер.
     WHERE event_object_table = tablename 
     -- Цикл LOOP пока не удалятся все таблицы с таким названием 
     LOOP
        EXECUTE 'DROP TRIGGER IF EXISTS ' || TriggerToDrop || ' ON ' || tablename;
        DeletedTriggersCount := DeletedTriggersCount + 1;
     END LOOP;
     -- Выводим количество удаленных триггеров
     RAISE NOTICE 'Deleted % triggers', DeletedTriggersCount;
END;
$$
LANGUAGE plpgsql;

-- Проверяем  наличие триггера
SELECT trigger_name FROM information_schema.triggers WHERE event_object_table = 'test_table';
CALL DropTrigger('test_table');

------------------------------------------------------------------------------------------------------------------------
-- 4) Create a stored procedure with an input parameter that outputs names and descriptions of object types
-- that have a string specified by the procedure parameter.
CREATE OR REPLACE PROCEDURE description_functiobs(
  IN string VARCHAR,
  IN OUT cursor refcursor
) AS $$
  BEGIN
      OPEN cursor FOR
        SELECT routine_name AS object_name,
               routine_type AS object_type
        FROM information_schema.routines
        WHERE specific_schema = 'public'
          AND routine_definition LIKE concat('%', string, '%');
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL description_functiobs('SELECT ', 'cursor');
FETCH ALL FROM "cursor";
END;
