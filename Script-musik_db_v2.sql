create table if not exists genres (
	id serial primary key,
	name varchar(50) not null unique
);

create table if not exists singers (
	id serial primary key,
	name varchar(150) not null unique
);

create table if not exists GenresSingers (
	id serial primary key,
	genre_id integer references genres(id),
	singer_id integer references singers(id)	 
);

create table if not exists albums (
	id serial primary key,
	name varchar(150) not null unique,
	release_year integer not null
);

create table if not exists AlbumsSingers (
	id serial primary key,
	album_id integer references albums(id),
	singer_id integer references singers(id)
);

create table if not exists tracks (
	id serial primary key,
	name varchar(150) not null unique,
	album_id integer references albums(id),
	track_length numeric(10) check(track_length > 0)
);

create table if not exists music_collection (
	id serial primary key,
	name varchar(150) not null unique,
	release_year integer not null
);

create table if not exists CollectionTracks (
	id serial primary key,
	collection_id integer references music_collection(id),
	track_id integer references tracks(id)
);