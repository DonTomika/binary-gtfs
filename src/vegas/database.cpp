#include <fstream>

#include "database.h"

#define debug_log(...) do { fprintf(stderr, __VA_ARGS__); putc('\n', stderr); } while (0);
#define error_log(...) do { fprintf(stderr, __VA_ARGS__); putc('\n', stderr); } while (0);

const Database *db_core::database = NULL;
char *db_core::buffer = NULL;

void db_core::load_database(const char *filename)
{
    if (database)
        unload_database();

    std::ifstream input(filename, std::ios::in | std::ios::binary | std::ios::ate);

    std::ifstream::pos_type size = input.tellg();
    input.seekg(0, std::ios::beg);

    if (size < sizeof(Database))
        return;

    Database *database = new Database;

    input.read((char *) database, sizeof(Database));

    if (database->sign != 0x44425653) // 'DBVS'
    {
        error_log("Database signature mismatch");
        delete database;
        return;
    }

    if (database->min_version > 4)
    {
        error_log("Database version mismatch");
        delete database;
        return;
    }

    if (database->file_size != size)
    {
        error_log("Database file size mismatch");
        delete database;
        return;
    }

    input.seekg(0, std::ios::beg);

    char *buffer = new char[(unsigned int) size];

    input.read(buffer, size);

    debug_log("DB preferred address: 0x%08X", database->preferred_address);
    debug_log("DB loaded address: 0x%08X", buffer);

    relocate((Database *) buffer, (size_t) buffer - database->preferred_address);

    debug_log("DB relocated");

    db_core::database = (Database *) buffer;
}

inline void relocate_field(void *p, int diff)
{
    *(size_t *) p = (*(size_t *) p ? ((*(size_t *) p) + diff) : 0);
}

void db_core::relocate(Database *db, int diff)
{
    debug_log("relocating database...");

    relocate_field(&db->calendar_overrides, diff);
    relocate_field(&db->calendar_school_holidays, diff);
    relocate_field(&db->day_types, diff);
    relocate_field(&db->route_categories, diff);
    relocate_field(&db->routes, diff);
    relocate_field(&db->stops, diff);
    relocate_field(&db->shapes, diff);

    debug_log("relocating calendar overrides...");

    for (uint16 i = 0; i < db->calendar_overrides.size(); i++)
    {
        CalendarOverrideEntry *calendar_override = &db->calendar_overrides[i];

        relocate_field(&calendar_override->description, diff);
    }

    debug_log("relocating calendar school holidays...");

    for (uint16 i = 0; i < db->calendar_school_holidays.size(); i++)
    {
        CalendarSchoolHolidayEntry *calendar_school_holiday = &db->calendar_school_holidays[i];

        relocate_field(&calendar_school_holiday->description, diff);
    }

    debug_log("relocating daytypes...");

    for (uint16 i = 0; i < db->day_types.size(); i++)
    {
        DayTypeEntry *day_type = &db->day_types[i];

        relocate_field(&day_type->name, diff);
        relocate_field(&day_type->rule, diff);
    }

    debug_log("relocating route categories...");

    for (uint16 i = 0; i < db->route_categories.size(); i++)
    {
        RouteCategoryEntry *category = &db->route_categories[i];

        relocate_field(&category->name, diff);
        relocate_field(&category->routes, diff);
    }

    debug_log("relocating routes...");

    for (uint16 i = 0; i < db->routes.size(); i++)
    {
        RouteEntry *route = &db->routes[i];
        
        relocate_field(&route->name, diff);
        relocate_field(&route->long_name, diff);
        relocate_field(&route->description, diff);
        relocate_field(&route->category, diff);
        relocate_field(&route->lines, diff);

        for (uint16 j = 0; j < route->lines.size(); j++)
        {
            RouteLineEntry *line = &route->lines[j];

            relocate_field(&line->route, diff);
            relocate_field(&line->name, diff);
            relocate_field(&line->continuation, diff);
            relocate_field(&line->headsigns, diff);
            relocate_field(&line->stops, diff);
            relocate_field(&line->travel_times, diff);
            relocate_field(&line->departure_groups, diff);

            for (uint16 k = 0; k < line->headsigns.size(); k++)
            {
                RouteHeadsignEntry *headsign = &line->headsigns[k];

                relocate_field(&headsign->name, diff);
            }

            for (uint16 k = 0; k < line->travel_times.size(); k++)
            {
                RouteTravelTimeEntry *travel_time = &line->travel_times[k];

                relocate_field(&travel_time->arrival_times, diff);
                relocate_field(&travel_time->departure_times, diff);
            }

            for (uint16 k = 0; k < line->departure_groups.size(); k++)
            {
                RouteDepartureGroupEntry *dep_1 = &line->departure_groups[k];

                relocate_field(&dep_1->time_offsets, diff);
            }
        }
    }

    debug_log("relocating stops...");

    for (uint16 i = 0; i < db->stops.size(); i++)
    {
        StopEntry *stop = &db->stops[i];

        relocate_field(&stop->name, diff);
        relocate_field(&stop->street, diff);
        relocate_field(&stop->subname, diff);
    }

    debug_log("relocating shapes...");

    for (uint16 i = 0; i < db->shapes.size(); i++)
    {
        ShapeEntry *shape = &db->shapes[i];

        relocate_field(&shape->packed_points, diff);
    }
}

void db_core::unload_database()
{
    if (database)
    {
        const_cast<Database * &>(database) = NULL;
        delete[] buffer;
        buffer = NULL;
    }
}
