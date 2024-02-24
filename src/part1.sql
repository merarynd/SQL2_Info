DROP DATABASE IF EXISTS s21info;

DROP TYPE IF EXISTS check_status CASCADE;

DROP TABLE IF EXISTS peers CASCADE;
DROP TABLE IF EXISTS tasks CASCADE;
DROP TABLE IF EXISTS checks CASCADE;
DROP TABLE IF EXISTS p2p CASCADE;
DROP TABLE IF EXISTS verter CASCADE;
DROP TABLE IF EXISTS transferredpoints CASCADE;
DROP TABLE IF EXISTS friends CASCADE;
DROP TABLE IF EXISTS recommendations CASCADE;
DROP TABLE IF EXISTS xp CASCADE;
DROP TABLE IF EXISTS timetracking CASCADE;

DROP PROCEDURE IF EXISTS ImportData CASCADE;
DROP PROCEDURE IF EXISTS ExportData CASCADE;


CREATE DATABASE s21info;

CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE peers (
    Nickname varchar not null primary key,
    Birthday date not null
);

CREATE TABLE tasks (
    Title varchar not null primary key,
    ParentTask varchar ,
    MaxXP integer,
    constraint fk_tasks_parent_task foreign key (ParentTask) references Tasks(Title)
);

CREATE TABLE checks (
    ID serial not null primary key ,
    Peer varchar not null ,
    Task varchar not null ,
    Date date,
    constraint fk_checks_task foreign key (Task) references Tasks(Title),
    constraint fk_checks_peer foreign key (Peer) references Peers(Nickname)
);

CREATE TABLE p2p (
    ID serial not null primary key,
    CheckID bigint,
    CheckingPeer varchar not null ,
    State check_status,
    Time time,
    constraint fk_p2p_check_id foreign key (CheckID) references Checks(ID),
    constraint fk_p2p_checking_peer foreign key (CheckingPeer) references Peers(Nickname)
);

CREATE TABLE verter (
    ID serial not null primary key ,
    CheckID bigint,
    State check_status,
    Time time,
    constraint fk_verter_check_id foreign key (CheckID) references Checks(ID)
);

CREATE TABLE transferredpoints (
    ID serial primary key ,
    CheckingPeer varchar not null ,
    CheckedPeer varchar not null ,
    PointsAmount integer not null ,
    constraint fk_transferred_points_checking_peer foreign key (CheckingPeer) references Peers(Nickname),
    constraint fk_transferred_points_checked_peer foreign key (CheckedPeer) references Peers(Nickname)
);

ALTER TABLE TransferredPoints ADD CONSTRAINT unique_transferred_points UNIQUE (CheckingPeer, CheckedPeer);

CREATE TABLE friends (
    ID serial primary key ,
    Peer1 varchar not null ,
    Peer2 varchar not null ,
    constraint fk_friends_peer1 foreign key (Peer1) references Peers(Nickname),
    constraint fk_friends_peer2 foreign key (Peer2) references Peers(Nickname)
);

CREATE TABLE recommendations (
    ID serial primary key ,
    Peer varchar not null ,
    RecommendedPeer varchar not null ,
    constraint fk_recommendations_peer foreign key (Peer) references Peers(Nickname),
    constraint fk_recommendations_recommended_peer foreign key (RecommendedPeer) references Peers(Nickname)
);

CREATE TABLE xp (
    ID serial primary key ,
    CheckID bigint,
    XPAmount integer,
    constraint fk_xp_check_id foreign key (CheckID) references Checks(ID)
);

CREATE TABLE timetracking (
    ID serial primary key,
    Peer varchar not null ,
    Date date,
    Time time,
    State integer,
    constraint fk_time_tracking_peer foreign key (Peer) references Peers(Nickname)
);

CREATE OR REPLACE PROCEDURE ImportData(IN table_name varchar, IN file_path varchar)
AS $$
    BEGIN
        EXECUTE 'SET datestyle TO "ISO, DMY"';
        EXECUTE format('COPY %I FROM %L WITH CSV HEADER DELIMITER '';''', table_name, file_path);
        EXECUTE 'RESET datestyle';
    END;
$$ LANGUAGE plpgsql;

                                               -- put your own file path here
CALL ImportData('peers', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/dataset_sql/peers.csv');
CALL ImportData('tasks', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/dataset_sql/tasks.csv');
CALL ImportData('checks', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/dataset_sql/checks.csv');
CALL ImportData('friends', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/dataset_sql/friends.csv');
CALL ImportData('p2p', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/dataset_sql/P2P.csv');
CALL ImportData('recommendations', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/dataset_sql/recommendations.csv');
CALL ImportData('timetracking', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/dataset_sql/time_tracking.csv');
CALL ImportData('transferredpoints', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/dataset_sql/transferred_points.csv');
CALL ImportData('verter', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/dataset_sql/verter.csv');
CALL ImportData('xp', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/dataset_sql/xp.csv');

CREATE OR REPLACE PROCEDURE ExportData(IN table_name varchar, IN file_path varchar)
AS $$
    BEGIN
        EXECUTE format('COPY %I TO %L WITH CSV HEADER DELIMITER '';''', table_name, file_path);
    END;
$$ LANGUAGE plpgsql;
                                               -- put your own file path here
CALL ExportData('peers', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/myset_sql/exported_peers.csv');
CALL ExportData('tasks', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/myset_sql/exported_tasks.csv');
CALL ExportData('checks', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/myset_sql/exported_checks.csv');
CALL ExportData('friends', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/myset_sql/exported_friends.csv');
CALL ExportData('p2p', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/myset_sql/exported_P2P.csv');
CALL ExportData('recommendations', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/myset_sql/exported_recommendations.csv');
CALL ExportData('timetracking', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/myset_sql/exported_time_tracking.csv');
CALL ExportData('transferredpoints', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/myset_sql/exported_transferred_points.csv');
CALL ExportData('verter', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/myset_sql/exported_verter.csv');
CALL ExportData('xp', '/Users/' || current_user || '/SQL2_Info21_v1.0-2/src/myset_sql/exported_xp.csv');