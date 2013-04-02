#include <stdio.h>

#include "vegas/database.h"

int main()
{
    db_core::load_database("example.db");

    // do the stuff

    db_core::unload_database();

    getchar();

    return 0;
}
