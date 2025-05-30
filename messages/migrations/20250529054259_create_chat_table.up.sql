-- Add up migration script here
CREATE TABLE "chat" (
    id SERIAL PRIMARY KEY,
    name VARCHAR UNIQUE,
    content TEXT
);