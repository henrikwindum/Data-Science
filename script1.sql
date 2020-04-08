CREATE TABLE IF NOT EXISTS Types (
  typeID SERIAL PRIMARY KEY,
  typeValue VARCHAR NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS Article (
  articleID INT NOT NULL PRIMARY KEY,
  summary VARCHAR,
  content VARCHAR,
  title VARCHAR,
  typeID INT NULL REFERENCES Types(typeID),
  insertedAt TIMESTAMP,
  lastUpdatedAt TIMESTAMP,
  scrapedAt TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Keyword (
  keywordID SERIAL PRIMARY KEY,
  keywordvalue VARCHAR NULL
);

CREATE TABLE IF NOT EXISTS Tags (
  keywordID INT REFERENCES Keyword(keywordID),
  articleID INT REFERENCES Article(articleID)
);

CREATE TABLE IF NOT EXISTS Urls (
  urlstring VARCHAR PRIMARY KEY,
  articleID INT REFERENCES Article(articleID),
  domain VARCHAR(45) NULL,
  source VARCHAR(45) NULL,
  metaDescription VARCHAR NULL,
  metaKeywords VARCHAR NULL
);

CREATE TABLE IF NOT EXISTS Author (
  authorID SERIAL PRIMARY KEY,
  authorName VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS WrittenBy (
  articleID INT REFERENCES Article(articleID),
  authorID INT REFERENCES Author(authorID)
);