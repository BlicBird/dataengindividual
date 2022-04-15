create schema football;

drop table if exists football.players cascade;
create table football.players (
    playerId                 varchar(128) primary key, 
    name                     varchar(256)
);

drop table if exists football.player_info cascade;
create table football.player_info (
    playerId                varchar(128) references football.players("playerId") on delete cascade on update cascade,
    height                  varchar(128),
    weight                  varchar(128),
    age                     varchar(128),
    position                varchar(128),
    shirt_no                varchar(128),
    apps                    varchar(128),
    date_joined             varchar(128)
);

drop table if exists football.stats cascade;
create table football.stats (
    playerId                varchar(128) references football.players("playerId") on delete cascade on update cascade,
    goals                   varchar(128),
    assists                 varchar(128),
    tackles                 varchar(128),
    shots                   varchar(128),
    keyPasses               varchar(128)
);

drop table if exists football.player_personal cascade;
create table football.player_personal (
    playerId                varchar(128) references football.players("playerId") on delete cascade on update cascade,
    birthdate               varchar(128),
    birth_country           varchar(128),
    birth_place             varchar(128),
    national_team           varchar(128)
);

drop table if exists football.tweet_counts cascade;
create table football.tweet_counts (
    playerId                varchar(128) references football.players("playerId") on delete cascade on update cascade,
    tweet_date              varchar(128),
    Tweet_count             varchar(128)
);

drop table if exists football.tweets cascade;
create table football.tweets (
    playerId                varchar(128) references football.players("playerId") on delete cascade on update cascade,
    Tweet                   varchar(128),
    Tweet_id                varchar(128) primary key
);