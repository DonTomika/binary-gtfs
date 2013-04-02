#include <iostream>
#include <time.h>
#include <soci.h>
#include <soci-sqlite3.h>

#include "common.h"
#include "db-structure.h"

std::string replaceAll(const std::string& str, const std::string& from, const std::string& to)
{
    std::string result = str;

    if (from.empty())
        return result;

    size_t start_pos = 0;
    while ((start_pos = result.find(from, start_pos)) != std::string::npos)
    {
        result.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }

    return result;
}

uint32 hex_to_uint32(const std::string& hex)
{
    uint32 result;

    std::stringstream ss;
    ss << std::hex << hex;
    ss >> result;

    return result;
}

Database *generate_db(std::string connection_string)
{
    soci::session sql;

    try
    {
        std::cout << "generating database from '" << connection_string << "'..." << std::endl;

        sql.open(soci::sqlite3, connection_string);
    }
    catch (soci::soci_error& e)
    {
        std::cout << "connecton error: " << e.what() << std::endl;
        return NULL;
    }

    //sql << "set names utf8";

    Database *db = new Database();

    std::cout << "initializing a new database with the default values..." << std::endl;
    {
        db->sign = 'DBVS';
        db->gen_version = 4;
        db->min_version = 4;
        db->content_version = 1;

        db->file_size = 0;
        db->file_crc = 0;
        db->preferred_address = 0;
        db->features = (Features) 1;

        db->generated_on = (uint32) time(NULL);
        db->valid_from = (uint32) time(NULL);
        db->valid_until = 0;

        db->calendar_overrides.init(nullptr, 0);
        db->calendar_school_holidays.init(nullptr, 0);
        db->day_types.init(nullptr, 0);
        db->route_categories.init(nullptr, 0);
        db->routes.init(nullptr, 0);
        db->stops.init(nullptr, 0);
        db->shapes.init(nullptr, 0);

        for (int i = 0; i < sizeof(db->reserved); i++)
            db->reserved[i] = 0;

        soci::rowset<soci::row> rs = (sql.prepare << "select content_version, valid_from, valid_until from metadata");

        for (auto it = rs.begin(); it != rs.end(); ++it)
        {
            soci::row const& row = *it;

            db->content_version = row.get<int>(0);
            db->valid_from = row.get<int>(1);
            db->valid_until = row.get<int>(2);
        }
    }

    std::map<int, int> stop_index_by_id;
    std::map<int, int> route_id_by_index;
    std::map<std::pair<int, int>, int> route_id_and_line_index_to_line_id;
    std::map<std::pair<int, int>, int> stop_times_count_by_route_id_and_line_index;

    std::cout << "loading stops..." << std::endl;
    {
        std::vector<StopEntry> stops;

        soci::rowset<soci::row> rs = (sql.prepare << "select id, name, subname, street, group_id, latitude, longitude, flags from stops order by id");

        int index = 0;
        for (auto it = rs.begin(); it != rs.end(); ++it)
        {
            soci::row const& row = *it;

            StopEntry stop_entry;
            stop_entry.id = index + 1;
            stop_entry.name = _strdup(row.get<std::string>(1).c_str());
            stop_entry.subname = _strdup(row.get<std::string>(2).c_str());
            stop_entry.street = _strdup(row.get<std::string>(3).c_str());
            stop_entry.group_id = row.get<int>(4);
            stop_entry.location.latitude = row.get<int>(5);
            stop_entry.location.longitude = row.get<int>(6);
            stop_entry.flags = row.get<int>(7);

            stops.push_back(stop_entry);

            stop_index_by_id[row.get<int>(0)] = index++;
        }

        StopEntry *temp = new StopEntry[stops.size()];
        for (size_t i = 0; i < stops.size(); i++)
            temp[i] = stops[i];

        db->stops.init(temp, stops.size());
    }

    std::cout << "loading calendar_override..." << std::endl;
    {
        std::vector<CalendarOverrideEntry> calendar_overrides;

        soci::rowset<soci::row> rs = (sql.prepare << "select id, from_date, to_date, new_type, description from calendar_override order by id");

        for (auto it = rs.begin(); it != rs.end(); ++it)
        {
            soci::row const& row = *it;

            CalendarOverrideEntry calendar_override_entry;
            calendar_override_entry.id = row.get<int>(0);
            calendar_override_entry.from_date = atoi(replaceAll(row.get<std::string>(1), "-", "").c_str());
            calendar_override_entry.to_date = atoi(replaceAll(row.get<std::string>(2), "-", "").c_str());
            calendar_override_entry.new_type = row.get<int>(3);
            calendar_override_entry.description = _strdup(row.get<std::string>(4).c_str());

            calendar_overrides.push_back(calendar_override_entry);
        }

        CalendarOverrideEntry *temp = new CalendarOverrideEntry[calendar_overrides.size()];
        for (size_t i = 0; i < calendar_overrides.size(); i++)
            temp[i] = calendar_overrides[i];

        db->calendar_overrides.init(temp, calendar_overrides.size());
    }

    std::cout << "loading calendar_school_holidays..." << std::endl;
    {
        std::vector<CalendarSchoolHolidayEntry> calendar_school_holidays;

        soci::rowset<soci::row> rs = (sql.prepare << "select id, from_date, to_date, description from calendar_school_holidays order by id");

        for (auto it = rs.begin(); it != rs.end(); ++it)
        {
            soci::row const& row = *it;

            CalendarSchoolHolidayEntry calendar_school_holiday_entry;
            calendar_school_holiday_entry.id = row.get<int>(0);
            calendar_school_holiday_entry.from_date = atoi(replaceAll(row.get<std::string>(1), "-", "").c_str());
            calendar_school_holiday_entry.to_date = atoi(replaceAll(row.get<std::string>(2), "-", "").c_str());
            calendar_school_holiday_entry.description = _strdup(row.get<std::string>(3).c_str());

            calendar_school_holidays.push_back(calendar_school_holiday_entry);
        }

        CalendarSchoolHolidayEntry *temp = new CalendarSchoolHolidayEntry[calendar_school_holidays.size()];
        for (size_t i = 0; i < calendar_school_holidays.size(); i++)
            temp[i] = calendar_school_holidays[i];

        db->calendar_school_holidays.init(temp, calendar_school_holidays.size());
    }

    std::cout << "loading day_types..." << std::endl;
    {
        std::vector<DayTypeEntry> day_types;

        soci::rowset<soci::row> rs = (sql.prepare << "select id, name, rule from day_types order by id");

        for (auto it = rs.begin(); it != rs.end(); ++it)
        {
            soci::row const& row = *it;

            DayTypeEntry day_type_entry;
            day_type_entry.id = row.get<int>(0);
            day_type_entry.name = ""; //_strdup(row.get<std::string>(1).c_str());
            day_type_entry.rule = _strdup(row.get<std::string>(2).c_str());

            day_types.push_back(day_type_entry);
        }

        DayTypeEntry *temp = new DayTypeEntry[day_types.size()];
        for (size_t i = 0; i < day_types.size(); i++)
            temp[i] = day_types[i];

        db->day_types.init(temp, day_types.size());
    }

    std::cout << "loading route_categrories..." << std::endl;
    {
        std::vector<RouteCategoryEntry> route_categories;

        soci::rowset<soci::row> rs = (sql.prepare << "select id, name, color, flags from route_categories order by id");

        for (auto it = rs.begin(); it != rs.end(); ++it)
        {
            soci::row const& row = *it;

            RouteCategoryEntry route_category_entry;
            route_category_entry.id = row.get<int>(0);
            route_category_entry.name = _strdup(row.get<std::string>(1).c_str());
            route_category_entry.color = hex_to_uint32(row.get<std::string>(2));
            route_category_entry.flags = row.get<int>(3);

            route_categories.push_back(route_category_entry);
        }

        RouteCategoryEntry *temp = new RouteCategoryEntry[route_categories.size()];
        for (size_t i = 0; i < route_categories.size(); i++)
            temp[i] = route_categories[i];

        db->route_categories.init(temp, route_categories.size());
    }

    std::cout << "loading routes..." << std::endl;
    {
        std::vector<RouteEntry> routes;

        soci::rowset<soci::row> rs = (sql.prepare << "select id, name, long_name, description, color, category_id from routes order by category_id, id");

        int index = 0;
        for (auto it = rs.begin(); it != rs.end(); ++it)
        {
            soci::row const& row = *it;

            RouteEntry route_entry;
            route_entry.id = index + 1;
            route_entry.name = _strdup(row.get<std::string>(1).c_str());
            route_entry.long_name = _strdup(row.get<std::string>(2).c_str());
            route_entry.description = _strdup(row.get<std::string>(3).c_str());
            route_entry.color = hex_to_uint32(row.get<std::string>(4));
            route_entry.category = nullptr;
            route_entry.lines.init(nullptr, 0);

            int category_id = row.get<int>(5);

            for (size_t i = 0; i < db->route_categories.size(); i++)
            {
                if (db->route_categories[i].id == category_id)
                {
                    route_entry.category = &db->route_categories[i];
                    break;
                }
            }

            routes.push_back(route_entry);

            route_id_by_index[index] = row.get<int>(0);

            index++;
        }

        RouteEntry *temp = new RouteEntry[routes.size()];
        for (size_t i = 0; i < routes.size(); i++)
            temp[i] = routes[i];

        db->routes.init(temp, routes.size());
    }

    std::cout << "loading route_lines..." << std::endl;
    {
        for (int i = 0; i < db->routes.size(); i++)
        {
            RouteEntry *route = &db->routes[i];

            int route_id = route_id_by_index[i];

            soci::rowset<soci::row> rs = (sql.prepare << "select line_id, direction, name, stop_times_count from route_lines where route_id = :route_id order by line_id", soci::use(route_id));

            std::vector<RouteLineEntry> route_lines;

            int index = 0;
            for (auto it = rs.begin(); it != rs.end(); ++it)
            {
                soci::row const& row = *it;

                RouteLineEntry route_line_entry;
                route_line_entry.route = route;
                route_line_entry.direction = row.get<int>(1);
                route_line_entry.name = _strdup(row.get<std::string>(2).c_str());
                route_line_entry.continuation = nullptr; // TODO
                route_line_entry.flags = 0;
                route_line_entry.headsigns.init(nullptr, 0);
                route_line_entry.stops.init(nullptr, 0);
                route_line_entry.travel_times.init(nullptr, 0);
                route_line_entry.departure_groups.init(nullptr, 0);

                route_lines.push_back(route_line_entry);

                route_id_and_line_index_to_line_id[std::pair<int, int>(route->id, index)] = row.get<int>(0);
                stop_times_count_by_route_id_and_line_index[std::pair<int, int>(route->id, index)] = row.get<int>(3);

                index++;
            }

            RouteLineEntry *temp = new RouteLineEntry[route_lines.size()];
            for (size_t i = 0; i < route_lines.size(); i++)
                temp[i] = route_lines[i];

            route->lines.init(temp, route_lines.size());
        }
    }

    std::cout << "loading route_headsigns..." << std::endl;
    {
        for (int i = 0; i < db->routes.size(); i++)
        {
            RouteEntry *route = &db->routes[i];

            int route_id = route_id_by_index[i];

            for (int j = 0; j < route->lines.size(); j++)
            {
                RouteLineEntry *line = &route->lines[j];

                int line_id = route_id_and_line_index_to_line_id.find(std::pair<int, int>(route->id, j))->second;

                soci::rowset<soci::row> rs = (sql.prepare << "select stop_index, headsign from route_headsigns where route_id = :route_id and line_id = :line_id order by stop_index", soci::use(route_id), soci::use(line_id));

                std::vector<RouteHeadsignEntry> headsigns;

                for (auto it = rs.begin(); it != rs.end(); ++it)
                {
                    soci::row const& row = *it;

                    RouteHeadsignEntry route_headsign_entry;
                    route_headsign_entry.stop_index = row.get<int>(0);
                    route_headsign_entry.name = _strdup(row.get<std::string>(1).c_str());

                    headsigns.push_back(route_headsign_entry);
                }

                RouteHeadsignEntry *temp = new RouteHeadsignEntry[headsigns.size()];
                for (size_t i = 0; i < headsigns.size(); i++)
                    temp[i] = headsigns[i];

                line->headsigns.init(temp, headsigns.size());
            }
        }
    }

    std::cout << "loading route_stops..." << std::endl;
    {
        for (int i = 0; i < db->routes.size(); i++)
        {
            RouteEntry *route = &db->routes[i];

            int route_id = route_id_by_index[i];

            for (int j = 0; j < route->lines.size(); j++)
            {
                RouteLineEntry *line = &route->lines[j];

                int line_id = route_id_and_line_index_to_line_id.find(std::pair<int, int>(route->id, j))->second;

                soci::rowset<soci::row> rs = (sql.prepare << "select stop_id, distance_traveled from route_stops where route_id = :route_id and line_id = :line_id order by stop_index", soci::use(route_id), soci::use(line_id));

                std::vector<RouteStopEntry> route_stops;

                for (auto it = rs.begin(); it != rs.end(); ++it)
                {
                    soci::row const& row = *it;

                    RouteStopEntry route_stop_entry;

                    int stop_id = row.get<int>(0);
                    auto itr = stop_index_by_id.find(stop_id);
                    if (itr != stop_index_by_id.end())
                        route_stop_entry.stop_id = itr->second + 1; // = &db->stops[itr->second];
                    else
                    {
                        route_stop_entry.stop_id = 0;
                        std::cout << " ## invalid row: route_stops[" << route->id << ", " << line_id << "], no stop found with id " << stop_id << std::endl;
                    }

                    route_stop_entry.distance_traveled = row.get<int>(1);

                    route_stops.push_back(route_stop_entry);
                }

                RouteStopEntry *temp = new RouteStopEntry[route_stops.size()];
                for (size_t i = 0; i < route_stops.size(); i++)
                    temp[i] = route_stops[i];

                line->stops.init(temp, route_stops.size());
            }
        }
    }

    std::cout << "loading stop times..." << std::endl;
    {
        for (int i = 0; i < db->routes.size(); i++)
        {
            RouteEntry *route = &db->routes[i];

            int route_id = route_id_by_index[i];

            for (int j = 0; j < route->lines.size(); j++)
            {
                RouteLineEntry *line = &route->lines[j];

                int line_id = route_id_and_line_index_to_line_id.find(std::pair<int, int>(route->id, j))->second;

                int stop_times_count = stop_times_count_by_route_id_and_line_index.find(std::pair<int, int>(route->id, j))->second;

                std::ostringstream ss;

                for (int k = 0; k < 100; k++)
                    ss << "arrival_time_" << k + 1 << ", ";

                for (int k = 0; k < 100; k++)
                    ss << "departure_time_" << k + 1 << ", ";

                std::string stuff = ss.str().substr(0, ss.str().length() - 2);

                soci::rowset<soci::row> rs = (sql.prepare << "select " + stuff + " from route_stops where route_id = :route_id and line_id = :line_id order by stop_index", soci::use(route_id), soci::use(line_id));

                RouteTravelTimeEntry rtts[100];
                uint16 *arrival_times[100];
                uint16 *departure_times[100];
                
                for (int k = 0; k < stop_times_count; k++)
                {
                    arrival_times[k] = new uint16[line->stops.size()];
                    departure_times[k] = new uint16[line->stops.size()];
                    rtts[k].arrival_times = arrival_times[k];
                    rtts[k].departure_times = departure_times[k];
                }

                int index = 0;
                for (auto it = rs.begin(); it != rs.end(); ++it)
                {
                    soci::row const& row = *it;

                    for (int k = 0; k < stop_times_count; k++)
                    {
                        arrival_times[k][index] = row.get<int>(k) / 2;
                        departure_times[k][index] = row.get<int>(k + 100) / 2;
                    }

                    index++;
                }

                RouteTravelTimeEntry *travel_times = new RouteTravelTimeEntry[stop_times_count];

                for (int k = 0; k < stop_times_count; k++)
                {
                    travel_times[k] = rtts[k];
                }
                
                line->travel_times.init(travel_times, stop_times_count);
            }
        }
    }

    std::cout << "loading departure times..." << std::endl;
    {
        for (int i = 0; i < db->routes.size(); i++)
        {
            RouteEntry *route = &db->routes[i];

            int route_id = route_id_by_index[i];

            for (int j = 0; j < route->lines.size(); j++)
            {
                RouteLineEntry *line = &route->lines[j];

                int line_id = route_id_and_line_index_to_line_id.find(std::pair<int, int>(route->id, j))->second;

                std::vector<RouteDepartureGroupEntry> dep_1s;

                soci::rowset<soci::row> rs = (sql.prepare << "select distinct day_type, time_id, flags from route_departures where route_id = :route_id and line_id = :line_id order by day_type, time_id, flags", soci::use(route_id), soci::use(line_id));

                int count = 0;

                for (auto it = rs.begin(); it != rs.end(); ++it)
                {
                    soci::row const& row = *it;

                    RouteDepartureGroupEntry dep_1;
                    dep_1.day_type_id = row.get<int>(0);
                    dep_1.time_id = row.get<int>(1);
                    dep_1.flags = row.get<int>(2);

                    std::vector<uint16> departures;

                    soci::rowset<soci::row> rs3 = (sql.prepare << "select time_offset from route_departures where route_id = :route_id and line_id = :line_id and day_type = :day_type and time_id = :time_id and flags = :flags order by time_offset", soci::use(route_id), soci::use(line_id), soci::use(dep_1.day_type_id), soci::use(dep_1.time_id), soci::use(dep_1.flags));

                    for (auto it3 = rs3.begin(); it3 != rs3.end(); ++it3)
                    {
                        soci::row const& row3 = *it3;

                        uint16 time_offset = row3.get<int>(0) / 2;

                        departures.push_back(time_offset);

                        count++;
                    }

                    uint16 *temp = new uint16[departures.size()];
                    for (size_t i = 0; i < departures.size(); i++)
                        temp[i] = departures[i];

                    dep_1.time_offsets.init(temp, departures.size());
                    dep_1s.push_back(dep_1);
                }

                RouteDepartureGroupEntry *temp = new RouteDepartureGroupEntry[dep_1s.size()];
                for (size_t i = 0; i < dep_1s.size(); i++)
                    temp[i] = dep_1s[i];

                line->departure_groups.init(temp, dep_1s.size());

                //std::cout << " ?? " << "added " << count << " departures to line " << route_id << ":" << line_id << std::endl;
            }
        }
    }

    std::cout << "loading shapes..." << std::endl;
    {
        std::vector<ShapeEntry> shapes;

        //soci::rowset<soci::row> rs = (sql.prepare << "select distinct stop_id, next_stop_id, next_segment from (select stop_id, (select stop_id from route_stops b where b.route_id = a.route_id and b.line_id = a.line_id and b.stop_index = a.stop_index + 1) as next_stop_id, next_segment from route_stops a) c where c.next_stop_id is not null and c.next_segment <> '' order by stop_id, next_stop_id");
        soci::rowset<soci::row> rs = (sql.prepare << "select stop_id, next_stop_id, next_segment from (select stop_id, (select stop_id from route_stops b where b.route_id = a.route_id and b.line_id = a.line_id and b.stop_index = a.stop_index + 1) as next_stop_id, next_segment, (select count(*) from route_departures d where d.route_id = a.route_id and d.line_id = a.line_id) as dep_nums from route_stops a) c where c.next_stop_id is not null and c.next_segment <> '' group by stop_id, next_stop_id, next_segment order by stop_id, next_stop_id, sum(dep_nums) desc");

        int last_from = 0;
        int last_to = 0;

        for (auto it = rs.begin(); it != rs.end(); ++it)
        {
            soci::row const& row = *it;

            std::string next_segment = row.get<std::string>(2);
            int count = (next_segment.length() + 1) / 18;

            if (count > 0)
            {
                ShapeEntry shape_entry;

                shape_entry.first_stop_id = row.get<int>(0);
                shape_entry.next_stop_id = row.get<int>(1); // TODO: long long?

                if (last_from == shape_entry.first_stop_id && last_to == shape_entry.next_stop_id)
                    continue;

                last_from = shape_entry.first_stop_id;
                last_to = shape_entry.next_stop_id;

                uint8 *packed_points = new uint8[8 + (count - 1) * 4];

                int32 last_lat = 0;
                int32 last_lon = 0;
                for (int k = 0; k < count; k++)
                {
                    int32 lat = atoi(next_segment.substr(k * 18, k * 18 + 8).c_str());
                    int32 lon = atoi(next_segment.substr(k * 18 + 9, k * 18 + 17).c_str());

                    if (k == 0)
                    {
                        memcpy(packed_points + 0, &lat, 4);
                        memcpy(packed_points + 4, &lon, 4);
                    }
                    else
                    {
                        int16 dlat = lat - last_lat;
                        int16 dlon = lon - last_lon;

                        memcpy(packed_points + 8 + (k - 1) * 4 + 0, &dlat, 2);
                        memcpy(packed_points + 8 + (k - 1) * 4 + 2, &dlon, 2);

                        //uint32 diff = ((lat - last_lat) << 16) + ((lon - last_lon) & 0xFFFF);
                        //memcpy(packed_points + 8 + (k - 1) * 4, &diff, 4);
                    }

                    last_lat = lat;
                    last_lon = lon;
                }

                shape_entry.point_count = count;
                shape_entry.packed_points = packed_points;

                shapes.push_back(shape_entry);
            }
        }

        ShapeEntry *temp = new ShapeEntry[shapes.size()];
        for (size_t i = 0; i < shapes.size(); i++)
            temp[i] = shapes[i];

        db->shapes.init(temp, shapes.size());
    }

    std::cout << "matching routes and categories..." << std::endl;
    {
        for (int i = 0; i < db->route_categories.size(); i++)
        {
            RouteEntry *address = nullptr;
            uint32 count = 0;

            for (int j = 0; j < db->routes.size(); j++)
            {
                if (address)
                {
                    if (&db->route_categories[i] == db->routes[j].category)
                    {
                        count++;
                        continue;
                    }
                    else
                    {
                        db->route_categories[i].routes.init(address, count);
                        break;
                    }
                }

                if (&db->route_categories[i] == db->routes[j].category)
                {
                    address = &db->routes[j];
                }
            }
        }
    }

    return db;
}
