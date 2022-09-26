
library(tidyverse)
library(RPostgres)
library(lubridate)
#install.packages('chron')
#library(chron)

setwd('C:\\Users\\yangm\\Desktop\\5310\\Group Data')

#CREATE THE DATABASE----
#_create a connection to kindle database----
con <- dbConnect(
  drv = dbDriver('Postgres'), 
  dbname = 'fest9',
  host = 'db-postgresql-nyc1-44203-do-user-8018943-0.b.db.ondigitalocean.com', 
  port = 25060,
  user = 'proj22b_9', 
  password = 'AVNS_xbfH3k5_6JaC27KJplN'
)



#_create artists table----
stmt <- 'CREATE TABLE artists (
          artist_id int,
          name varchar(50),
          nationality varchar(50),
          gender varchar(1),
          PRIMARY KEY(artist_id)
        );'
#stmt <- 'Drop Table samples'
#stmt <- 'Drop Table show_art'
#stmt <- 'Drop Table artists'
dbExecute(con, stmt)  

#_create locations table----
stmt <- 'CREATE TABLE locations (
          locn_id int,
          address varchar(150),
          lat decimal(8,6),
          lng decimal(9,6),
          PRIMARY KEY(locn_id)
        );'




#_create hotels table----
stmt <- 'CREATE TABLE hotels (
          hotel_id int,
          hotel_name varchar(50),
          hotel_star int,
          hotel_website varchar(100),
          price_range int,
          locn_id int,
          PRIMARY KEY(hotel_id),
          FOREIGN KEY(locn_id) REFERENCES locations 
        );'
dbExecute(con, stmt)  

##dbExecute(con, stmt)  

#stmt <- 'ALTER TABLE hotels ALTER COLUMN hotel_name TYPE VARCHAR(100)'
dbExecute(con, stmt)  


#_create amenities table----
stmt <- 'CREATE TABLE amenities (
          amen_id int,
          amen_type varchar(50),
          PRIMARY KEY(amen_id)
        );'
dbExecute(con, stmt)  


#_create hotel amenities table----
stmt <- 'CREATE TABLE hotel_amnt (
          hotel_id int,
          amen_id int,
          PRIMARY KEY(hotel_id, amen_id),
          FOREIGN KEY(hotel_id) REFERENCES hotels,
          FOREIGN KEY(amen_id) REFERENCES amenities
        );'
dbExecute(con, stmt)



#_create attractions table----
stmt <- 'CREATE TABLE attractions (
          attr_id int,
          attr_type varchar(25),
          attr_name varchar(50),
          locn_id int,
          opening_hr time,
          closing_hr time,
          price int,
          PRIMARY KEY(attr_id),
          FOREIGN KEY(locn_id) REFERENCES locations
        );'

#stmt <- 'Drop Table attractions'
dbExecute(con, stmt)  


#_create restaurants table----
stmt <- 'CREATE TABLE restaurants (
          rest_id int,
          locn_id int, 
          rest_name varchar(50),
          rest_type varchar(50),
          rest_price char(1),
          open_hr time,
          close_hr time,
          rest_ratings decimal(2,1),
          rest_website varchar(500),
          PRIMARY KEY(rest_id),
          FOREIGN KEY(locn_id) REFERENCES locations
        );'

#stmt <- 'Drop Table restaurants'
dbExecute(con, stmt)




#_create samples table----
stmt <- 'CREATE TABLE samples (
          sample_id int,
          artist_id int,
          video_id varchar(20),
          video_URL varchar (100),
          video_name varchar (100),
          PRIMARY KEY(sample_id),
          FOREIGN KEY(artist_id) REFERENCES artists
          
        );'

#stmt <- 'Drop Table samples'
dbExecute(con, stmt)  


#_create bus_food_tour table----
stmt <- 'CREATE TABLE bus_food_tour (
          bus_id int,
          rest_id int,
          start_time time, 
          end_time time, 
          rest_type varchar(10),
          dep_id int,
          ter_id int,
          ticket_price varchar(10),
          PRIMARY KEY(bus_id),
          FOREIGN KEY(dep_id) REFERENCES locations,
          CONSTRAINT fk1 FOREIGN KEY (dep_id) REFERENCES locations(locn_id),
          CONSTRAINT fk2 FOREIGN KEY (ter_id) REFERENCES locations(locn_id)
        );'
dbExecute(con, stmt)

 

#_create shows table----
stmt <- 'CREATE TABLE shows (
          show_id int,
          show_dt timestamp,
          locn_id int,
          
          PRIMARY KEY(show_id),
          FOREIGN KEY(locn_id) REFERENCES locations
          
        );'
dbExecute(con, stmt)  

#_create show_art table----
stmt <- 'CREATE TABLE show_art (
          show_id int,
          artist_id int,
          PRIMARY KEY(show_id, artist_id),
          CONSTRAINT fk1 FOREIGN KEY (show_id) REFERENCES shows(show_id),
          CONSTRAINT fk2 FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
          
        );'
dbExecute(con, stmt) 


#_create tour_att table----
stmt <- 'CREATE TABLE tour_att (
          bus_id int, 
          attr_id int, 
          PRIMARY KEY(bus_id, attr_id),
          FOREIGN KEY(bus_id) REFERENCES bus_food_tour,
          FOREIGN KEY(attr_id) REFERENCES attractions
        );'
#stmt <- 'Drop Table tour_att'
dbExecute(con, stmt)  

#_create tickets table----
stmt <- 'CREATE TABLE tickets (
          ticket_id int, 
          price int, 
          PRIMARY KEY(ticket_id)
        );'
dbExecute(con, stmt)


#_create show_ticket table----
stmt <- 'CREATE TABLE show_ticket (
          show_id int, 
          ticket_id int, 
          PRIMARY KEY(show_id, ticket_id),
          FOREIGN KEY(show_id) REFERENCES shows,
          FOREIGN KEY(ticket_id) REFERENCES tickets
        );'
dbExecute(con, stmt)  

#******************----
#LOAD----

# read data
df_amt <- read.csv('amt.csv')
df_artists <- read.csv('artists.csv')
df_attractions <- read.csv('attractions.csv')
df_hotelamt <- read.csv('hotel_amt.csv')
df_hotels <- read.csv('hotels.csv')
df_locns <- read.csv('locns.csv')
df_res <- read.csv('res.csv')
df_samples <- read.csv('samples.csv')
df_showart <- read.csv('show_art.csv')
df_showticket <- read.csv('show_ticket.csv')
df_shows <- read.csv('shows.csv')
df_tickets <- read.csv('tickets.csv')
df_tour <- read.csv('bus_tour.csv')
df_tourattr <- read.csv('tour_attr.csv')
# _load amenities table----

dbWriteTable(
  conn = con,
  name = 'amenities',
  value = df_amt,
  row.names = FALSE,
  append = TRUE
)
# _load hotels table----


df_hotels2 <- df_hotels %>% 
  select (hotel_id,hotel_name,hotel_star,hotel_website, price_range,locn_id) %>%
  distinct()
dbWriteTable(
  conn = con,
  name = 'hotels',
  value = df_hotels2,
  row.names = FALSE,
  append = TRUE
)
# _load locations table----

dbWriteTable(
  conn = con,
  name = 'locations',
  value = df_locns,
  row.names = FALSE,
  append = TRUE
)



df_update <- data.frame(locn_id = 143, 
                        address = "Bd de Parc, 77700 Coupvray, France", 
                        lat = 48.88638,
                        lng = 2.7469598)
dbWriteTable(con, "locations", df_update, append = TRUE, row.names = FALSE)


# _load attractions table----

df_attractions2 <- df_attractions %>% 
  select (attr_id,attr_type,attr_name,
          locn_id, opening_hr, closing_hr,price) %>%
  distinct()
dbWriteTable(
  conn = con,
  name = 'attractions',
  value = df_attractions2,
  row.names = FALSE,
  append = TRUE
)



df_update <- data.frame(attr_id = 21,locn_id = 143, 
                        attr_name = "Disneyland Paris",
                        attr_type = "Amusement Park",
                        price = 125,
                        opening_hr = '9:30',
                        closing_hr = '23:00')
dbWriteTable(con, "attractions", df_update, append = TRUE, row.names = FALSE)



# _load hotel_amnt table----
dbWriteTable(
  conn = con,
  name = 'hotel_amnt',
  value = df_hotelamt,
  row.names = FALSE,
  append = TRUE
)

# _load restaurants table----


df_res2 <- df_res %>% 
  select (rest_id,locn_id,rest_name,
          rest_type, rest_price,open_hr, close_hr,rest_ratings, rest_website) %>%
  distinct()
dbWriteTable(
  conn = con,
  name = 'restaurants',
  value = df_res2,
  row.names = FALSE,
  append = TRUE
)

#_load artists table----
df_artists2 <- df_artists %>% 
  select (artist_id,name,
          gender, nationality) %>%
  distinct()
dbWriteTable(
  conn = con,
  name = 'artists',
  value = df_artists2,
  row.names = FALSE,
  append = TRUE
)
#df_samples$sample_id <- df_samples$ï..sample_id
#_load samples table----
df_samples2 <- df_samples %>% 
  select (sample_id,video_id,
          artist_id, video_url, video_name) %>%
  distinct()

dbWriteTable(
  conn = con,
  name = 'samples',
  value = df_samples2,
  row.names = FALSE,
  append = TRUE
)

#_load shows table----

dbWriteTable(
  conn = con,
  name = 'shows',
  value = df_shows,
  row.names = FALSE,
  append = TRUE
)

#_load tickets table----
dbWriteTable(
  conn = con,
  name = 'tickets',
  value = df_tickets,
  row.names = FALSE,
  append = TRUE
)

#_load show_ticket table----

dbWriteTable(
  conn = con,
  name = 'show_ticket',
  value = df_showticket,
  row.names = FALSE,
  append = TRUE
)

#_load show_ticket table----
dbWriteTable(
  conn = con,
  name = 'show_art',
  value = df_showart,
  row.names = FALSE,
  append = TRUE
)

#_load tour table----
df_tour$bus_id <- df_tour$ï..bus_id
df_tour2 <- df_tour %>% 
  select (bus_id,rest_id,
          start_time, end_time,rest_type, dep_id, ter_id, ticket_price) %>%
  distinct()

dbWriteTable(
  conn = con,
  name = 'bus_food_tour',
  value = df_tour2,
  row.names = FALSE,
  append = TRUE
)


df_update <- data.frame(bus_id = 5,
                        rest_id = 45, 
                        dep_id = 143,
                        ter_id = 139,
                        rest_type = "dinner",
                        ticket_price = 100,
                        start_time = '18:00',
                        end_time = '20:00')
dbWriteTable(con, "bus_food_tour", df_update, append = TRUE, row.names = FALSE)


#_load tour_att table----
df_tourattr$bus_id <- df_tourattr$ï..bus_id
df_tourattr2 <- df_tourattr %>%
    select(bus_id, attr_id)
dbWriteTable(
  conn = con,
  name = 'tour_att',
  value = df_tourattr2,
  row.names = FALSE,
  append = TRUE
)

df_update <- data.frame(bus_id = 5,
                        attr_id = 13)
dbWriteTable(con, "tour_att", df_update, append = TRUE, row.names = FALSE)
