/*
Schema Contents.
------------------

- Created 6 tables.
- Created columns with correct data types.
- Defined primary keys for each table.
- Added NOT NULL constraints.
- Added DROP TABLE IF EXISTS statements.
- Added simple constraints.
- Defined foreign keys.
- Created functions to implement complex constraints.
*/

DROP TABLE IF EXISTS Act CASCADE;
CREATE TABLE Act(
    actID SERIAL NOT NULL,
    actname VARCHAR(100) NOT NULL,
    genre VARCHAR(10) NOT NULL,
    standardfee INT NOT NULL,
    PRIMARY KEY (actID),
    UNIQUE (actname),
    CHECK (standardfee >= 0)
);

DROP TABLE IF EXISTS venue CASCADE;
CREATE TABLE venue(
    venueid SERIAL NOT NULL,
    venuename VARCHAR(100) NOT NULL,
    hirecost INT NOT NULL,
    capacity INT NOT NULL,
    PRIMARY KEY (venueid),
    UNIQUE (venuename),
    CHECK (hirecost >= 0)
);

DROP TABLE IF EXISTS gig CASCADE;
CREATE TABLE gig(
    gigID SERIAL NOT NULL,
    venueid INT NOT NULL REFERENCES venue(venueid),
    gigtitle VARCHAR(100) NOT NULL,
    gigdate TIMESTAMP NOT NULL,
    gigstatus VARCHAR(10) NOT NULL,
    PRIMARY KEY (gigID),
    CHECK (gigstatus = 'Cancelled' OR gigstatus = 'GoingAhead')
);

DROP TABLE IF EXISTS act_gig CASCADE;
CREATE TABLE act_gig(
    actID INT NOT NULL REFERENCES Act(actID),
    gigID INT NOT NULL REFERENCES gig(gigID) ON DELETE CASCADE,
    actfee INT NOT NULL,
    ontime TIMESTAMP NOT NULL,
    duration INT NOT NULL,
    PRIMARY KEY (actID, gigID, ontime),
    CHECK (actfee >= 0)
);

DROP TABLE IF EXISTS gig_ticket CASCADE;
CREATE TABLE gig_ticket(
    gigID INT NOT NULL REFERENCES gig(gigID) ON DELETE CASCADE,
    pricetype VARCHAR(2) NOT NULL,
    price INT NOT NULL,
    PRIMARY KEY (gigID, pricetype),
    CHECK (price >= 0)
);

DROP TABLE IF EXISTS ticket CASCADE;
CREATE TABLE ticket(
    ticketid SERIAL NOT NULL,
    gigID INT NOT NULL,
    pricetype VARCHAR(2) NOT NULL,
    Cost INT NOT NULL,
    CustomerName VARCHAR(100) NOT NULL,
    CustomerEmail VARCHAR(100) NOT NULL,
    PRIMARY KEY (ticketid),
    FOREIGN KEY (gigID, pricetype) REFERENCES gig_ticket(gigID, pricetype) ON DELETE CASCADE
);

/*Function to calculate when a performance ends.*/
CREATE OR REPLACE FUNCTION fn_endtime(start_date_time TIMESTAMP, dur INT)
    RETURNS TIMESTAMP AS
$$
DECLARE
    interval_string VARCHAR;
BEGIN
    interval_string := CONCAT(dur, ' MINUTES');
    RETURN start_date_time + interval_string::INTERVAL;
END;
$$
LANGUAGE PLPGSQL;

/*Function to check if a performance ends before midnight.*/
CREATE OR REPLACE FUNCTION fn_ends_before_midnight(start_date_time TIMESTAMP, dur INT)
    RETURNS INT AS
$$
DECLARE
    end_date_time TIMESTAMP;
BEGIN
    end_date_time := fn_endtime(start_date_time, dur);
    IF end_date_time::DATE = start_date_time::DATE THEN
        RETURN 1;
    ELSE
        RETURN 0;
    END IF;
END;
$$
LANGUAGE PLPGSQL;

/*Function to check if act ontime is at or after the gig gigdate.*/
CREATE OR REPLACE FUNCTION fn_check_ontime_after_gigdate(id INT, date_time TIMESTAMP)
    RETURNS INT AS
$$
DECLARE
    start_date_time TIMESTAMP;
BEGIN
    SELECT gigdate INTO start_date_time FROM gig WHERE gigID = id;
    IF date_time >= start_date_time THEN
        RETURN 1;
    ELSE
        RETURN 0;
    END IF;
END;
$$
LANGUAGE PLPGSQL;

/*Function to calculate when a gig ends.*/
CREATE OR REPLACE FUNCTION fn_gig_endtime(id INT)
    RETURNS TIMESTAMP AS
$$
DECLARE
    this_act RECORD;
    this_endtime TIMESTAMP;
    gig_endtime TIMESTAMP;
BEGIN
    SELECT gigdate
    INTO gig_endtime
    FROM gig
    WHERE gigID = id;
    FOR this_act IN SELECT ontime, duration FROM act_gig WHERE gigID = id
        LOOP
        this_endtime := fn_endtime(this_act.ontime, this_act.duration);
        IF this_endtime > gig_endtime THEN
            gig_endtime := this_endtime;
        END IF;
        END LOOP;
    RETURN gig_endtime;
END;
$$
LANGUAGE PLPGSQL;

/*Function to check that different gigs at same venue leave 3 hour gap.     NOT USED !!!!!!!!!!!!!!!    */
CREATE OR REPLACE FUNCTION fn_no_gig_clash(v_id INT)
    RETURNS INT AS
$$
DECLARE
    no_clash INT;
    this_gig RECORD;
    this_gig_2 RECORD;
    this_end TIMESTAMP;
    this_max TIMESTAMP;
BEGIN
    no_clash := 1;
    FOR this_gig IN SELECT gigID, gigdate FROM gig WHERE venueid = v_id
        LOOP
        this_end := fn_gig_endtime(this_gig.gigID);
        this_max := this_end + INTERVAL '3 Hours';
        FOR this_gig_2 IN SELECT gigID, gigdate FROM gig WHERE venueid = v_id
            LOOP
            IF this_gig_2.gigID <> this_gig.gigID AND this_gig_2.gigdate >= this_gig.gigdate AND this_gig_2.gigdate < this_max THEN
                no_clash := 0;
            END IF;
            END LOOP;
        END LOOP;
    RETURN no_clash;
END;
$$
LANGUAGE PLPGSQL;

/*Function to check that acts at same gig don't clash, first act starts at gigdate, and max gap in lineup is 20 minutes.*/
CREATE OR REPLACE FUNCTION fn_following_act_rules(g_id INT)
    RETURNS INT AS
$$
DECLARE
    followed_rules INT;
    no_clash INT;
    small_gaps INT;
    gigdate_start INT;
    this_act RECORD;
    this_act_2 RECORD;
    this_end TIMESTAMP;
    this_gap INTERVAL;
    this_small_gaps INT;
    this_gaps INT;
    g_date TIMESTAMP;
    verdict INT;
    first_start TIMESTAMP;
BEGIN
    SELECT gigdate
    INTO g_date
    FROM gig
    WHERE gigID = g_id;
    no_clash := 1;
    small_gaps := 1;
    gigdate_start := 0;
    FOR this_act IN SELECT actID, ontime, duration FROM act_gig WHERE gigID = g_id
        LOOP
        this_small_gaps := 0;
        this_gaps := 0;
        this_end := fn_endtime(this_act.ontime, this_act.duration);
        FOR this_act_2 IN SELECT actID, ontime FROM act_gig WHERE gigID = g_id
            LOOP
            IF this_act_2.actID <> this_act.actID OR this_act_2.ontime <> this_act.ontime THEN
                IF this_act_2.ontime >= this_act.ontime AND this_act_2.ontime < this_end THEN
                    no_clash := 0;
                END IF;
                IF this_act_2.ontime >= this_end THEN
                    this_gaps := 1;
                    this_gap := this_act_2.ontime - this_end;
                    IF this_gap <= '20 Minutes'::INTERVAL THEN
                        this_small_gaps := 1;
                    END IF;
                END IF;
            END IF;
            END LOOP;
        IF this_gaps = 1 AND this_small_gaps = 0 THEN
            small_gaps = 0;
        END IF;
        END LOOP;
    SELECT ontime
    INTO first_start
    FROM act_gig
    WHERE gigID = g_id
    ORDER BY ontime
    LIMIT 1;
    IF first_start = g_date THEN
        gigdate_start := 1;
    END IF;
    IF no_clash = 1 AND small_gaps = 1 AND gigdate_start = 1 THEN
        verdict := 1;
    ELSE
        verdict := 0;
    END IF;
    RETURN verdict;
END;
$$
LANGUAGE PLPGSQL;

/*Function to get price of particular gig ticket.*/
CREATE OR REPLACE FUNCTION fn_get_price(id INT, ptype VARCHAR)
    RETURNS INT AS
$$
BEGIN
    RETURN price FROM gig_ticket WHERE gigID = id AND pricetype = ptype;
END;
$$
LANGUAGE PLPGSQL;

/*Function to get capacity of gig.*/
CREATE OR REPLACE FUNCTION fn_get_capacity(id INT)
    RETURNS INT AS
$$
BEGIN
    RETURN capacity FROM venue NATURAL JOIN gig WHERE gigID = id;
END;
$$
LANGUAGE PLPGSQL;

/*Function to check number of tickets.*/
CREATE OR REPLACE FUNCTION fn_check_tickets(id INT)
    RETURNS INT AS
$$
DECLARE
    count INT;
    this_ticket RECORD;
BEGIN
    count := 0;
    FOR this_ticket IN SELECT * FROM ticket WHERE gigID = id
        LOOP
        count := count + 1;
        END LOOP;
    RETURN count;
END;
$$
LANGUAGE PLPGSQL;

/*Function to check if names are consistent with a particular email address.*/
CREATE OR REPLACE FUNCTION fn_name_match_email(addr VARCHAR)
    RETURNS INT AS
$$
DECLARE
    matches INT;
    this_person VARCHAR;
    first_record INT;
    p_name VARCHAR;
BEGIN
    matches := 1;
    first_record := 1;
    FOR this_person IN SELECT CustomerName FROM ticket WHERE CustomerEmail = addr
        LOOP
        IF first_record = 1 THEN
            first_record := 0;
            p_name := this_person;
        ELSEIF this_person <> p_name THEN
            matches := 0;
        END IF;
        END LOOP;
    RETURN matches;
END;
$$
LANGUAGE PLPGSQL;

/*Function to find headline act.*/
CREATE OR REPLACE FUNCTION fn_headline(id INT)
    RETURNS TIMESTAMP AS
$$
DECLARE
    this_perf RECORD;
    start_date_time TIMESTAMP;
    headliner TIMESTAMP;
BEGIN
    SELECT gigdate
    INTO start_date_time
    FROM gig
    WHERE gigID = id;
    headliner := start_date_time;
    FOR this_perf IN SELECT ontime FROM act_gig WHERE gigID = id
        LOOP
        IF this_perf.ontime >= headliner THEN
            headliner := this_perf.ontime;
        END IF;
        END LOOP;
    RETURN headliner;
END;
$$
LANGUAGE PLPGSQL;

/*ALTER TABLE act_gig ADD CONSTRAINT check_lineup_rules CHECK (fn_following_act_rules(gigID) = 1);*/
ALTER TABLE act_gig ADD CONSTRAINT check_ebm CHECK (fn_ends_before_midnight(ontime, duration) = 1);
ALTER TABLE act_gig ADD CONSTRAINT check_after_gigdate CHECK (fn_check_ontime_after_gigdate(gigID, ontime) = 1);
ALTER TABLE ticket ADD CONSTRAINT check_sold_out CHECK (fn_check_tickets(gigID) < fn_get_capacity(gigID));

/*Views and function for option 7.*/
CREATE VIEW headliners AS SELECT gigID, actID, EXTRACT(YEAR FROM gigdate) AS year FROM gig NATURAL JOIN act_gig WHERE gigstatus = 'GoingAhead' AND fn_headline(gigID) = ontime;
CREATE VIEW customers AS SELECT gigID, actID, year, CustomerEmail FROM headliners NATURAL JOIN ticket;
CREATE OR REPLACE FUNCTION fn_regular(id INT, email VARCHAR)
    RETURNS INT AS
$$
DECLARE
    this_year INT;
    regular INT;

BEGIN
    regular := 1;
    FOR this_year IN SELECT DISTINCT year FROM customers WHERE actID = id
        LOOP
        IF email NOT IN (SELECT CustomerEmail FROM customers WHERE actID = id AND year = this_year) THEN
            regular := 0;
        END IF;
        END LOOP;
    RETURN regular;
END; 
$$
LANGUAGE PLPGSQL 