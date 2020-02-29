
create schema flightstats;

create table flightstats.airports
(
  iata        char(3),
  name        varchar(150),
  citycode    char(3),
  city        varchar(100),
  countrycode char(3),
  countryname varchar(100)
);

create unique index airports_iata_uindex
   on flightstats.airports (iata);

create table flightstats.aircrafts
(
    flightnumber    char(4),
    iata            char(3),
    name            varchar(150),
    economy         integer,
    premium_economy integer,
    business        integer,
    first           integer
);

create table flightstats.flights
(
    upload_at               timestamp,
    carrierfscode           char(2),
    flightnumber            char(4),
    departureairportfscode  char(3),
    arrivalairportfscode    char(3),
    departuretime           timestamp,
    arrivaltime             timestamp,
    flightequipmentiatacode char(3)
);
