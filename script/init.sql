
-- USER SQL
CREATE USER usr_score IDENTIFIED BY "usr_score"  
DEFAULT TABLESPACE "USERS"
TEMPORARY TABLESPACE "TEMP";

-- ROLES
GRANT DBA TO usr_score ;

-- TABLES
CREATE TABLE usr_score.company (
    name       NVARCHAR2(25) NOT NULL,
    company_id NUMBER NOT NULL
);

COMMENT ON TABLE usr_score.company IS
    'Company entity';

COMMENT ON COLUMN usr_score.company.name IS
    'Company name';

COMMENT ON COLUMN usr_score.company.company_id IS
    'Company ID';

ALTER TABLE usr_score.company ADD CONSTRAINT company_pk PRIMARY KEY ( company_id );

ALTER TABLE usr_score.company ADD CONSTRAINT company_name_un UNIQUE ( name );

CREATE TABLE usr_score.console (
    name       NVARCHAR2(25) NOT NULL,
    console_id NUMBER NOT NULL,
    company_id NUMBER NOT NULL
);

COMMENT ON TABLE usr_score.console IS
    'Console entity';

COMMENT ON COLUMN usr_score.console.name IS
    'Console name';

COMMENT ON COLUMN usr_score.console.console_id IS
    'Console ID';

COMMENT ON COLUMN usr_score.console.company_id IS
    'Company ID';

ALTER TABLE usr_score.console ADD CONSTRAINT console_pk PRIMARY KEY ( console_id );

ALTER TABLE usr_score.console ADD CONSTRAINT score_name_un UNIQUE ( name );

CREATE TABLE usr_score.run_in (
    videogame_id NUMBER NOT NULL,
    console_id   NUMBER NOT NULL
);

COMMENT ON COLUMN usr_score.run_in.videogame_id IS
    'Videogame ID';

COMMENT ON COLUMN usr_score.run_in.console_id IS
    'Console ID';

ALTER TABLE usr_score.run_in ADD CONSTRAINT run_in_pk PRIMARY KEY ( videogame_id,
                                                                    console_id );

CREATE TABLE usr_score.score (
    userscore    NUMBER(2, 1) NULL,
    metascore    NUMBER(3) NOT NULL,
    registration_date       DATE NOT NULL,
    videogame_id NUMBER NOT NULL,
    console_id NUMBER NOT NULL
);

COMMENT ON TABLE usr_score.score IS
    'Score entity';

COMMENT ON COLUMN usr_score.score.userscore IS
    'Game score given by users';

COMMENT ON COLUMN usr_score.score.metascore IS
    'Game score given by Metacritic';

COMMENT ON COLUMN usr_score.score.registration_date IS
    'Score date';

COMMENT ON COLUMN usr_score.score.videogame_id IS
    'Videogame ID';

COMMENT ON COLUMN usr_score.score.console_id IS
    'Console ID';

ALTER TABLE usr_score.score ADD CONSTRAINT score_pk PRIMARY KEY ( videogame_id,
                                                                  console_id,
                                                                  registration_date );

CREATE TABLE usr_score.videogame (
    name         NVARCHAR2(120) NOT NULL,
    videogame_id NUMBER NOT NULL
);

COMMENT ON TABLE usr_score.videogame IS
    'Videogame entity';

COMMENT ON COLUMN usr_score.videogame.name IS
    'Game name';

COMMENT ON COLUMN usr_score.videogame.videogame_id IS
    'Videogame ID';

ALTER TABLE usr_score.videogame ADD CONSTRAINT videogame_pk PRIMARY KEY ( videogame_id );

ALTER TABLE usr_score.videogame ADD CONSTRAINT videogame_name_un UNIQUE ( name );

ALTER TABLE usr_score.console
    ADD CONSTRAINT console_company_fk FOREIGN KEY ( company_id )
        REFERENCES usr_score.company ( company_id );

ALTER TABLE usr_score.run_in
    ADD CONSTRAINT run_in_console_fk FOREIGN KEY ( console_id )
        REFERENCES usr_score.console ( console_id );

ALTER TABLE usr_score.run_in
    ADD CONSTRAINT run_in_videogame_fk FOREIGN KEY ( videogame_id )
        REFERENCES usr_score.videogame ( videogame_id );

ALTER TABLE usr_score.score
    ADD CONSTRAINT score_run_in_fk FOREIGN KEY ( videogame_id, console_id )
        REFERENCES usr_score.run_in ( videogame_id, console_id );

CREATE SEQUENCE usr_score.company_company_id_seq START WITH 1 NOCACHE ORDER;

CREATE OR REPLACE TRIGGER usr_score.company_company_id_trg BEFORE
    INSERT ON usr_score.company
    FOR EACH ROW
    WHEN ( new.company_id IS NULL )
BEGIN
    :new.company_id := usr_score.company_company_id_seq.nextval;
END;
/


CREATE SEQUENCE usr_score.console_console_id_seq START WITH 1 NOCACHE ORDER;

CREATE OR REPLACE TRIGGER usr_score.console_console_id_trg BEFORE
    INSERT ON usr_score.console
    FOR EACH ROW
    WHEN ( new.console_id IS NULL )
BEGIN
    :new.console_id := usr_score.console_console_id_seq.nextval;
END;
/

CREATE SEQUENCE usr_score.videogame_videogame_id_seq START WITH 1 NOCACHE ORDER;

CREATE OR REPLACE TRIGGER usr_score.videogame_videogame_id_trg BEFORE
    INSERT ON usr_score.videogame
    FOR EACH ROW
    WHEN ( new.videogame_id IS NULL )
BEGIN
    :new.videogame_id := usr_score.videogame_videogame_id_seq.nextval;
END;
/

CREATE USER staging IDENTIFIED BY "staging"  
DEFAULT TABLESPACE "USERS"
TEMPORARY TABLESPACE "TEMP";

grant create table to staging;
alter user staging quota unlimited on users;

CREATE TABLE staging.consoles_csv (
    console         NVARCHAR2(25),
    company         NVARCHAR2(25),
    created_date    DATE DEFAULT sysdate NOT NULL 
);

CREATE TABLE staging.result_csv (
    metascore	NUMBER(3,0),
    name        NVARCHAR2(120),
    console     NVARCHAR2(25),
    userscore   NUMBER(3,0),
    "date"        DATE,
    created_date DATE DEFAULT sysdate NOT NULL 
);

CREATE VIEW usr_score.videogame_ranking_by_console AS
SELECT cm.name company, c.name console, v.name videogame, AVG(metascore) ranking
FROM usr_score.score s
INNER JOIN usr_score.console c ON c.console_id=s.console_id
INNER JOIN usr_score.company cm ON c.company_id=cm.company_id
INNER JOIN usr_score.videogame v ON v.videogame_id=s.videogame_id
GROUP BY cm.name, c.name, v.name;

CREATE VIEW usr_score.videogame_ranking AS
SELECT v.name videogame, AVG(metascore) ranking
FROM usr_score.score s
INNER JOIN usr_score.videogame v ON v.videogame_id=s.videogame_id
GROUP BY v.name;