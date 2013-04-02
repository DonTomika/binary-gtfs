#ifndef VEGAS_DB_SERIALIZER_H
#define VEGAS_DB_SERIALIZER_H

#include "common.h"
#include "db-structure.h"

void serialize_database(const char *filename, Database *db);

#endif
