#include <iostream>

#include "db_generator.h"
#include "db_serializer.h"

int main(int argc, char *argv[])
{
    if (argc == 3)
    {
        Database *db = generate_db(argv[1]);

        if (db)
            serialize_database(argv[2], db);

        return 0;
    }
    else
    {
        std::cout << "parameters are incorrect, check the source for help" << std::endl;

        return 1;
    }
}
