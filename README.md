# OpenAQ-projekti
Ilmanlaatudatasovellus. Ohjelma kysyy käyttäjältä inputin ja listaa kaikki kaupungin sisällä olevat mittauspisteet. Vielä tällä hetkellä ohjelma lataa tiedot kaikista mittauspisteistä, eikä kysy erikseen mistä pisteestä tieto haetaan.

# Tietokannan käyttäjän oikeudet

lisää nämä:

GRANT ALL PRIVILEGES ON DATABASE tietokannan_nimi TO tietokannan_käyttäjä;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA tietokannan_nimi TO tietokannan_käyttäjä;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA tietokannan_nimi TO tietokannan_käyttäjä;

ALTER TABLE countries OWNER TO tietokannan_käyttäjä;

ALTER TABLE cities OWNER TO tietokannan_käyttäjä;

ALTER TABLE locations OWNER TO tietokannan_käyttäjä;

ALTER TABLE parameters OWNER TO tietokannan_käyttäjä;

ALTER TABLE sensors OWNER TO tietokannan_käyttäjä;

ALTER TABLE measurements OWNER TO tietokannan_käyttäjä;

ALTER SEQUENCE countries_id_seq OWNER TO tietokannan_käyttäjä;

ALTER SEQUENCE cities_id_seq OWNER TO tietokannan_käyttäjä;

ALTER SEQUENCE locations_id_seq OWNER TO tietokannan_käyttäjä;

ALTER SEQUENCE parameters_id_seq OWNER TO tietokannan_käyttäjä;

ALTER SEQUENCE sensors_id_seq OWNER TO tietokannan_käyttäjä;

ALTER SEQUENCE measurements_id_seq OWNER TO tietokannan_käyttäjä;
