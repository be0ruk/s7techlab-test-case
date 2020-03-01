
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
    airlinefscode   char(2),
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

create view flightstats.all_flights_data
            (upload_at, airline_code, flight_number, departure_time, departure_airport_code, departure_airport_name,
             departure_city, departure_contry, arrival_time, arrival_airport_code, arrival_airport_name, arrival_city,
             arrival_country, aircraft_type_code, aircraft_type_name, economy, premium_economy, business, first)
as
SELECT fl.upload_at,
       fl.carrierfscode           AS airline_code,
       fl.flightnumber            AS flight_number,
       fl.departuretime           AS departure_time,
       fl.departureairportfscode  AS departure_airport_code,
       ap_departure.name          AS departure_airport_name,
       ap_departure.city          AS departure_city,
       ap_departure.countryname   AS departure_contry,
       fl.arrivaltime             AS arrival_time,
       fl.arrivalairportfscode    AS arrival_airport_code,
       ap_arrival.name            AS arrival_airport_name,
       ap_arrival.city            AS arrival_city,
       ap_arrival.countryname     AS arrival_country,
       fl.flightequipmentiatacode AS aircraft_type_code,
       ac.name                    AS aircraft_type_name,
       ac.economy,
       ac.premium_economy,
       ac.business,
       ac.first
FROM flightstats.flights fl
         LEFT JOIN flightstats.airports ap_departure ON fl.departureairportfscode = ap_departure.iata
         LEFT JOIN flightstats.airports ap_arrival ON fl.arrivalairportfscode = ap_arrival.iata
         LEFT JOIN flightstats.aircrafts ac
                   ON fl.carrierfscode = ac.airlinefscode AND fl.flightnumber = ac.flightnumber;
