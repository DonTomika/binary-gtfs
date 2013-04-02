#ifndef VEGAS_DB_GENERATOR_H
#define VEGAS_DB_GENERATOR_H

#include <string>

#include "db-structure.h"

Database *generate_db(std::string connection_string);

#endif
