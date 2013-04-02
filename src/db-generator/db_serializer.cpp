#include <iostream>
#include <set>
#include <fstream>
#include <vector>
#include <list>
#include <algorithm>
#include <map>
#include <typeinfo>
#include <string.h>
#include <Windows.h>

#include "db_serializer.h"

struct string_compare
{
    bool operator() (const char * lhs, const char * rhs) const
    {
        return strcmp(lhs, rhs) < 0;
    }
};

class db_serializer
{
public:
    db_serializer()
    {
        register_structure<Database>(
            offsetof(Database, calendar_overrides),
            offsetof(Database, calendar_school_holidays),
            offsetof(Database, day_types),
            offsetof(Database, route_categories),
            offsetof(Database, routes),
            offsetof(Database, stops),
            offsetof(Database, shapes)
        );

        register_structure<CalendarOverrideEntry>(
            offsetof(CalendarOverrideEntry, description)
        );

        register_structure<CalendarSchoolHolidayEntry>(
            offsetof(CalendarSchoolHolidayEntry, description)
        );

        register_structure<DayTypeEntry>(
            offsetof(DayTypeEntry, name),
            offsetof(DayTypeEntry, rule)
        );

        register_structure<RouteCategoryEntry>(
            offsetof(RouteCategoryEntry, name),
            offsetof(RouteCategoryEntry, routes)
        );

        register_structure<RouteEntry>(
            offsetof(RouteEntry, name),
            offsetof(RouteEntry, long_name),
            offsetof(RouteEntry, description),
            offsetof(RouteEntry, category),
            offsetof(RouteEntry, lines)
        );

        register_structure<RouteLineEntry>(
            offsetof(RouteLineEntry, route),
            offsetof(RouteLineEntry, name),
            offsetof(RouteLineEntry, continuation),
            offsetof(RouteLineEntry, headsigns),
            offsetof(RouteLineEntry, stops),
            offsetof(RouteLineEntry, travel_times),
            offsetof(RouteLineEntry, departure_groups)
        );

        register_structure<RouteHeadsignEntry>(
            offsetof(RouteHeadsignEntry, name)
        );

        register_structure<RouteStopEntry>(
            //offsetof(RouteStopEntry, stop)
        );
        
        register_structure<RouteTravelTimeEntry>(
            offsetof(RouteTravelTimeEntry, arrival_times),
            offsetof(RouteTravelTimeEntry, departure_times)
        );

        register_structure<RouteDepartureGroupEntry>(
            offsetof(RouteDepartureGroupEntry, time_offsets)
        );

        register_structure<LocationEntry>(
        );

        register_structure<StopEntry>(
            offsetof(StopEntry, name),
            offsetof(StopEntry, subname),
            offsetof(StopEntry, street)
        );

        register_structure<ShapeEntry>(
            offsetof(ShapeEntry, packed_points)
        );
    }

    ~db_serializer()
    {
        // TODO: delete elements of structures
    }

    void serialize(Database *db, const char *filename)
    {
        initialize_buffer();

        std::cout << " >> starting export [" << offset << " bytes so far]..." << std::endl;

        // Database
        add_object(db);

        std::cout << " >> database written [" << offset << " bytes so far]" << std::endl;

        // CalendarOverrideEntry
        for (uint32 i = 0; i < db->calendar_overrides.size(); i++)
        {
            CalendarOverrideEntry *override = &db->calendar_overrides[i];

            add_object(override);

            add_string(override->description);
        }

        std::cout << " >> calendar overrides written [" << offset << " bytes so far]" << std::endl;

        // CalendarSchoolHolidayEntry
        for (uint32 i = 0; i < db->calendar_school_holidays.size(); i++)
        {
            CalendarSchoolHolidayEntry *holiday = &db->calendar_school_holidays[i];

            add_object(holiday);

            add_string(holiday->description);
        }

        std::cout << " >> calendar school holidays [" << offset << " bytes so far]" << std::endl;

        // DayTypeEntry
        for (uint32 i = 0; i < db->day_types.size(); i++)
        {
            DayTypeEntry *day_type = &db->day_types[i];

            add_object(day_type);

            add_string(day_type->name);
            add_string(day_type->rule);
        }

        std::cout << " >> day types written [" << offset << " bytes so far]" << std::endl;

        // RouteCategoryEntry
        for (uint32 i = 0; i < db->route_categories.size(); i++)
        {
            RouteCategoryEntry *category = &db->route_categories[i];

            add_object(category);

            add_string(category->name);
        }
        
        std::cout << " >> route categories written [" << offset << " bytes so far]" << std::endl;

        // RouteEntry
        for (uint32 i = 0; i < db->routes.size(); i++)
        {
            RouteEntry *route = &db->routes[i];

            add_object(route);

            add_string(route->name);
            add_string(route->long_name);
            add_string(route->description);
        }

        std::cout << " >> routes written [" << offset << " bytes so far]" << std::endl;

        // StopEntry
        for (uint32 i = 0; i < db->stops.size(); i++)
        {
            StopEntry *stop = &db->stops[i];

            add_object(stop);

            add_string(stop->name);
            add_string(stop->street);
            add_string(stop->subname);
        }

        std::cout << " >> stops written [" << offset << " bytes so far]" << std::endl;

        // RouteLineEntry
        for (uint32 i = 0; i < db->routes.size(); i++)
        {
            RouteEntry *route = &db->routes[i];

            for (uint32 j = 0; j < route->lines.size(); j++)
            {
                RouteLineEntry *line = &route->lines[j];

                add_object(line);

                add_string(line->name);
            }
        }

        std::cout << " >> route lines written [" << offset << " bytes so far]" << std::endl;

        // RouteHeadsignEntry
        for (uint32 i = 0; i < db->routes.size(); i++)
        {
            RouteEntry *route = &db->routes[i];

            for (uint32 j = 0; j < route->lines.size(); j++)
            {
                RouteLineEntry *line = &route->lines[j];

                for (uint32 k = 0; k < line->headsigns.size(); k++)
                {
                    RouteHeadsignEntry *headsign = &line->headsigns[k];

                    add_object(headsign);

                    add_string(headsign->name);
                }
            }
        }

        std::cout << " >> route headsigns written [" << offset << " bytes so far]" << std::endl;

        // RouteStopEntry
        for (uint32 i = 0; i < db->routes.size(); i++)
        {
            RouteEntry *route = &db->routes[i];

            for (uint32 j = 0; j < route->lines.size(); j++)
            {
                RouteLineEntry *line = &route->lines[j];

                for (uint32 k = 0; k < line->stops.size(); k++)
                {
                    RouteStopEntry *stop = &line->stops[k];

                    add_object(stop);
                }
            }
        }

        std::cout << " >> route stops written [" << offset << " bytes so far]" << std::endl;

        // RouteTravelTimeEntry
        for (uint32 i = 0; i < db->routes.size(); i++)
        {
            RouteEntry *route = &db->routes[i];

            for (uint32 j = 0; j < route->lines.size(); j++)
            {
                RouteLineEntry *line = &route->lines[j];

                for (uint32 k = 0; k < line->travel_times.size(); k++)
                {
                    RouteTravelTimeEntry *travel_time = &line->travel_times[k];

                    add_object(travel_time);

                    add_array(travel_time->arrival_times, line->stops.size() * sizeof(uint16));
                    add_array(travel_time->departure_times, line->stops.size() * sizeof(uint16));
                }
            }
        }

        std::cout << " >> route travel times written [" << offset << " bytes so far]" << std::endl;

        // RouteDepartureGroupEntry
        for (uint32 i = 0; i < db->routes.size(); i++)
        {
            RouteEntry *route = &db->routes[i];

            for (uint32 j = 0; j < route->lines.size(); j++)
            {
                RouteLineEntry *line = &route->lines[j];

                for (uint32 k = 0; k < line->departure_groups.size(); k++)
                {
                    RouteDepartureGroupEntry *dep_1 = &line->departure_groups[k];

                    add_object(dep_1);

                    add_array(dep_1->time_offsets.base(), dep_1->time_offsets.size() * sizeof(uint16));
                }
            }
        }

        std::cout << " >> route departures written [" << offset << " bytes so far]" << std::endl;

        // ShapeEntry
        for (uint32 i = 0; i < db->shapes.size(); i++)
        {
            ShapeEntry *shape = &db->shapes[i];

            add_object(shape);

            add_array(shape->packed_points, 8 + (shape->point_count - 1) * 4);
        }

        std::cout << " >> shapes written [" << offset << " bytes so far]" << std::endl;

        dump_arrays();

        std::cout << " >> arrays written [" << offset << " bytes so far]" << std::endl;

        dump_strings();

        std::cout << " >> strings written [" << offset << " bytes so far]" << std::endl;

        relocate_pointers(buffer - (char *) db);

        ((Database *) buffer)->preferred_address = (uint32) buffer;

        ((Database *) buffer)->file_crc = 0;
        ((Database *) buffer)->file_size = offset;

        write_buffer(filename);

        free_buffer();

        std::cout << " >> done" << std::endl;
    }

private:
    template<typename T>
    void register_structure(size_t offset1, size_t offset2, size_t offset3, size_t offset4, size_t offset5, size_t offset6, size_t offset7, size_t offset8)
    {
        std::vector<size_t> *offsets = new std::vector<size_t>();

        offsets->push_back(offset1);
        offsets->push_back(offset2);
        offsets->push_back(offset3);
        offsets->push_back(offset4);
        offsets->push_back(offset5);
        offsets->push_back(offset6);
        offsets->push_back(offset7);
        offsets->push_back(offset8);

        structure_offsets[&typeid(T)] = offsets;
    }

    template<typename T>
    void register_structure(size_t offset1, size_t offset2, size_t offset3, size_t offset4, size_t offset5, size_t offset6, size_t offset7)
    {
        std::vector<size_t> *offsets = new std::vector<size_t>();

        offsets->push_back(offset1);
        offsets->push_back(offset2);
        offsets->push_back(offset3);
        offsets->push_back(offset4);
        offsets->push_back(offset5);
        offsets->push_back(offset6);
        offsets->push_back(offset7);

        structure_offsets[&typeid(T)] = offsets;
    }

    template<typename T>
    void register_structure(size_t offset1, size_t offset2, size_t offset3, size_t offset4, size_t offset5, size_t offset6)
    {
        std::vector<size_t> *offsets = new std::vector<size_t>();

        offsets->push_back(offset1);
        offsets->push_back(offset2);
        offsets->push_back(offset3);
        offsets->push_back(offset4);
        offsets->push_back(offset5);
        offsets->push_back(offset6);

        structure_offsets[&typeid(T)] = offsets;
    }

    template<typename T>
    void register_structure(size_t offset1, size_t offset2, size_t offset3, size_t offset4, size_t offset5)
    {
        std::vector<size_t> *offsets = new std::vector<size_t>();

        offsets->push_back(offset1);
        offsets->push_back(offset2);
        offsets->push_back(offset3);
        offsets->push_back(offset4);
        offsets->push_back(offset5);

        structure_offsets[&typeid(T)] = offsets;
    }

    template<typename T>
    void register_structure(size_t offset1, size_t offset2, size_t offset3, size_t offset4)
    {
        std::vector<size_t> *offsets = new std::vector<size_t>();

        offsets->push_back(offset1);
        offsets->push_back(offset2);
        offsets->push_back(offset3);
        offsets->push_back(offset4);

        structure_offsets[&typeid(T)] = offsets;
    }

    template<typename T>
    void register_structure(size_t offset1, size_t offset2, size_t offset3)
    {
        std::vector<size_t> *offsets = new std::vector<size_t>();

        offsets->push_back(offset1);
        offsets->push_back(offset2);
        offsets->push_back(offset3);

        structure_offsets[&typeid(T)] = offsets;
    }

    template<typename T>
    void register_structure(size_t offset1, size_t offset2)
    {
        std::vector<size_t> *offsets = new std::vector<size_t>();

        offsets->push_back(offset1);
        offsets->push_back(offset2);

        structure_offsets[&typeid(T)] = offsets;
    }

    template<typename T>
    void register_structure(size_t offset1)
    {
        std::vector<size_t> *offsets = new std::vector<size_t>();

        offsets->push_back(offset1);

        structure_offsets[&typeid(T)] = offsets;
    }

    template<typename T>
    void register_structure()
    {
        std::vector<size_t> *offsets = new std::vector<size_t>();

        structure_offsets[&typeid(T)] = offsets;
    }

    void initialize_buffer()
    {
        //buffer = new char[10 * 1000 * 1000];
        buffer = (char *) VirtualAlloc((void *) 0x49000000, 15 * 1000 * 1000, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);

        if (!buffer)
            std::cout << "allocation error: " << GetLastError() << std::endl;

        offset = 0;
    }

    template<typename T>
    bool add_object(T *ptr)
    {
        if (address_remaps.find((size_t) ptr) != address_remaps.end())
            return false;

        T *ptr2 = (T *) (buffer + offset);
        memcpy(buffer + offset, ptr, sizeof(T));

        address_types[buffer + offset] = &typeid(T);
        address_remaps[(size_t) ptr] = (size_t) (buffer + offset);

        offset += sizeof(T);

        return true;
    }

    void add_array(void *p, size_t size)
    {
        arrays.push_back(std::pair<void *, size_t>(p, size));
    }

    void add_string(const char *str)
    {
        strings.push_back(str);
    }

    const char *ConvertToUTF8(const char *pStr)
    {
        static char szBuf[1024];
        static wchar_t wcstring[1024];

        size_t convertedChars;

        mbstowcs_s(&convertedChars, wcstring, 1024, pStr, 1024);

        WideCharToMultiByte(CP_UTF8, 0, wcstring, -1, szBuf, sizeof(szBuf), NULL, NULL);

        return szBuf;
    }

    /*
     * The memmem() function finds the start of the first occurrence of the
     * substring 'needle' of length 'nlen' in the memory area 'haystack' of
     * length 'hlen'.
     *
     * The return value is a pointer to the beginning of the sub-string, or
     * NULL if the substring is not found.
     */
    void *memmem(const void *haystack, size_t hlen, const void *needle, size_t nlen)
    {
        int needle_first;
        const unsigned char *p = (const unsigned char *) haystack;
        size_t plen = hlen;

        if (!nlen)
            return NULL;

        needle_first = *(unsigned char *)needle;

        while (plen >= nlen && (p = (const unsigned char *) memchr(p, needle_first, plen - nlen + 1)))
        {
            if (!memcmp(p, needle, nlen))
                return (void *)p;

            p++;
            plen = hlen - (p - (const unsigned char *) haystack);
        }

        return NULL;
    }

    void dump_arrays()
    {
        /*for (arraylist::iterator itr = arrays.begin(); itr != arrays.end(); itr++)
        {
            void *p = itr->first;
            size_t size = itr->second;

            memcpy(buffer + offset, p, size);

            address_types[buffer + offset] = &typeid(*itr);
            address_remaps[(size_t) p] = (size_t) (buffer + offset);

            offset += size;
        }*/

        std::sort(arrays.begin(), arrays.end(), [](arraylist::value_type a, arraylist::value_type b) { return a.second > b.second; });

        arraylist final_arrays;

        // collect unique values
        for (size_t i = 0; i < arrays.size(); i++)
        {
            arraylist::value_type arr = arrays[i];

            bool found = false;
            for (size_t j = 0; j < final_arrays.size(); j++)
            {
                arraylist::value_type final_arr = final_arrays[j];

                if (void *match = memmem(final_arr.first, final_arr.second, arr.first, arr.second))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
                final_arrays.push_back(arr);
        }

        // write unique strings to the buffer
        for (size_t i = 0; i < final_arrays.size(); i++)
        {
            arraylist::value_type arr = final_arrays[i];

            memcpy(buffer + offset, arr.first, arr.second);

            address_types[buffer + offset] = &typeid(arr.first);
            address_remaps[(size_t) arr.first] = (size_t) (buffer + offset);

            offset += arr.second;
        }

        // add the rest to the remap table
        for (size_t i = 0; i < arrays.size(); i++)
        {
            arraylist::value_type arr = arrays[i];

            if (address_remaps.find((size_t) arr.first) == address_remaps.end())
            {
                for (size_t j = 0; j < final_arrays.size(); j++)
                {
                    arraylist::value_type final_arr = final_arrays[j];

                    if (void *match = memmem(final_arr.first, final_arr.second, arr.first, arr.second))
                    {
                        address_remaps[(size_t) arr.first] = address_remaps[(size_t) final_arr.first] + ((unsigned char *) match - (unsigned char *) final_arr.first);
                        break;
                    }
                }
            }
        }
    }

    const char *ends_with(const char *base, const char *sub)
    {
        if (!strcmp(base, sub))
            return base;

        if (const char *pos = strstr(base, sub))
            if (pos - base + strlen(sub) == strlen(base))
                return pos;

        return NULL;
    }

    void dump_strings()
    {
        std::sort(strings.begin(), strings.end(), [](const char *a, const char *b) { return strlen(a) > strlen(b); });

        std::vector<const char *> final_strings;

        // collect unique strings
        for (size_t i = 0; i < strings.size(); i++)
        {
            const char *str = strings[i];

            bool found = false;
            for (size_t j = 0; j < final_strings.size(); j++)
            {
                const char *str_final = final_strings[j];

                if (const char *match = ends_with(str_final, str))
                {
                    found = true;
                    break;
                }

            }

            if (!found)
                final_strings.push_back(str);
        }

        // write unique strings to the buffer
        for (size_t i = 0; i < final_strings.size(); i++)
        {
            const char *str = final_strings[i];
            int len = strlen(str) + 1;
            memcpy(buffer + offset, str, len);

            address_types[buffer + offset] = &typeid(str);
            address_remaps[(size_t) str] = (size_t) (buffer + offset);

            offset += len;
        }

        // add the rest to the remap table
        for (size_t i = 0; i < strings.size(); i++)
        {
            const char *str = strings[i];

            if (address_remaps.find((size_t) str) == address_remaps.end())
            {
                for (size_t j = 0; j < final_strings.size(); j++)
                {
                    const char *str_final = final_strings[j];
    
                    if (const char *match = ends_with(str_final, str))
                    {
                        address_remaps[(size_t) str] = address_remaps[(size_t) str_final] + (match - str_final);
                        break;
                    }
                }
            }
        }
    }

    void write_buffer(const char *filename)
    {
        std::ofstream output(filename, std::ios::out | std::ios::binary);

        output.write(buffer, offset);
    }

    void free_buffer()
    {
        //delete[] buffer;
    }

    void relocate_pointers(int diff)
    {
        for (std::map<void *, const std::type_info *>::iterator itr = address_types.begin(); itr != address_types.end(); ++itr)
        {
            void *address = itr->first;
            const std::type_info *type = itr->second;

            std::vector<size_t> *offsets = structure_offsets[type];

            if (offsets)
            {
                for (std::vector<size_t>::iterator itr2 = offsets->begin(); itr2 != offsets->end(); ++itr2)
                {
                    size_t offset = *itr2;

                    size_t old_content = *(size_t *)((char *) address + offset);

                    if (old_content != 0)
                    {
                        size_t new_content = address_remaps[old_content];

                        //*((size_t *) (((char *) address + diff) + offset)) = (size_t) new_ptr;
                        *((size_t *) ((size_t) address + offset)) = new_content;
                    }
                }
            }
        }
    }

    char *buffer;
    int offset;

    // TODO: get rid of this
    typedef std::vector<const char *> stringlist;
    stringlist strings;

    typedef std::vector<std::pair<void *, size_t> > arraylist;
    arraylist arrays;

    std::map<void *, const std::type_info *> address_types;
    std::map<size_t, size_t> address_remaps;
    std::map<const std::type_info *, std::vector<size_t> *> structure_offsets;
};

void serialize_database(const char *filename, Database *db)
{
    std::cout << "creating file '" << filename << "'..." << std::endl;

    db_serializer serializer;

    serializer.serialize(db, filename);
}
