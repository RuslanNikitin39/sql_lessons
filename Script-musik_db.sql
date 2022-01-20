create table if not exists genres (
	id serial primary key,
	name varchar(50) not null unique
);

create table if not exists singers (
	id serial primary key,
	name varchar(150) not null unique,
	genre_id integer references genres(id)
);

create table if not exists albums (
	id serial primary key,
	name varchar(150) not null unique,
	release_year integer(4) not null,
	singer_id integer references singers(id)
);

create table if not exists tracks (
	id serial primary key,
	name varchar(150) not null unique,
	album_id integer references albums(id),
	track_length numeric(10) check(track_length > 0)
);