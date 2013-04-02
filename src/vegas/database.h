#ifndef VEGAS_DATABASE_H
#define VEGAS_DATABASE_H

#include "common.h"
#include "db-structure.h"

class db_core
{
public:
    static const Database *database;

    static void load_database(const char *filename);
    static void unload_database();

private:
    static void relocate(Database *db, int diff);

    static char *buffer;
};

#endif
