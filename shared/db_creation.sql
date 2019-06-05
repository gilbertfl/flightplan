CREATE TABLE requests (
    [id] int IDENTITY(1,1) PRIMARY KEY, 
    [engine] varchar(255) NOT NULL, 
    [partners] bit NOT NULL, 
    [fromCity] varchar(255) NOT NULL, 
    [toCity] varchar(255) NOT NULL, 
    [departDate] varchar(255) NOT NULL, 
    [returnDate] varchar(255), 
    [cabin] varchar(255), 
    [quantity] int default 0, 
    [assets] varchar(255) NOT NULL, 
    [updatedAt] datetime default CURRENT_TIMESTAMP
);

CREATE TABLE awards (
    [id] int IDENTITY(1,1) PRIMARY KEY, 
    [requestId] int, 
    [engine] varchar(255) NOT NULL, 
    [partner] bit NOT NULL, 
    [fromCity] varchar(255) NOT NULL, 
    [toCity] varchar(255) NOT NULL, 
    [date] varchar(255) NOT NULL, 
    [cabin] varchar(255) NOT NULL, 
    [mixed] bit NOT NULL, 
    [duration] int, 
    [stops] int default 0, 
    [quantity] int default 1, 
    [mileage] int, 
    [fees] varchar(255), 
    [fares] varchar(255), 
    [updated_at] datetime default CURRENT_TIMESTAMP
);

CREATE TABLE segments (
    [id] int IDENTITY(1,1) PRIMARY KEY, 
    [awardId] int, 
    [position] int NOT NULL, 
    [airline] varchar(255) NOT NULL, 
    [flight] varchar(255) NOT NULL, 
    [aircraft] varchar(255), 
    [fromCity] varchar(255) NOT NULL, 
    [toCity] varchar(255) NOT NULL, 
    [date] varchar(255) NOT NULL, 
    [departure] varchar(255) NOT NULL, 
    [arrival] varchar(255) NOT NULL, 
    [duration] int, 
    [nextConnection] int, 
    [cabin] varchar(255), 
    [stops] int DEFAULT 0, 
    [lagDays] int DEFAULT 0, 
    [bookingCode] varchar(255), 
    [updated_at] datetime default CURRENT_TIMESTAMP
);