#ifndef VEGAS_DB_STRUCTURE_H
#define VEGAS_DB_STRUCTURE_H

#include "common.h"

#pragma pack(push)
#pragma pack(1)

template<typename T, typename S = uint32>
struct array
{
public:
    T& operator[] (int i) const { return _p[i]; }
    T *base() const { return _p; }
    S size() const { return _size; }
    void init(T *address, S size) { _p = address; _size = size; }

private:
    T *_p;
    S _size;
};

enum Features : uint32
{
    FEATURE_STOP_COORDINATES            = 0x00000001,
    FEATURE_SHAPES                      = 0x00000002,
};

enum StopFlags : uint8
{
    STOP_FLAG_HAVE_WHEELCHAIR_DATA      = 0x00000001,
    STOP_FLAG_WHEELCHAIR_ACCESSIBLE     = 0x00000002,
    STOP_FLAG_UNDERGORUND               = 0x00000004,
};

enum DepartureFlags : uint8
{
    DEPARTURE_FLAG_HAVE_WHEELCHAIR_DATA      = 0x00000001,
    DEPARTURE_FLAG_WHEELCHAIR_ACCESSIBLE     = 0x00000002,
};

struct CalendarOverrideEntry;
struct CalendarSchoolHolidayEntry;
struct DayTypeEntry;
struct RouteCategoryEntry;
struct RouteEntry;
struct RouteLineEntry;
struct RouteHeadsignEntry;
struct RouteStopEntry;
struct RouteTravelTimeEntry;
struct RouteDepartureGroupEntry;
struct LocationEntry;
struct StopEntry;
struct ShapeEntry;

typedef uint32 date;

struct Database // (0x4C - 76 bytes)
{
    uint32                                   sign;                              // 0x00
    uint8                                    gen_version;                       // 0x04
    uint8                                    min_version;                       // 0x05
    uint16                                   reserved;                          // 0x06
                                
    uint32                                   file_size;                         // 0x08
    uint32                                   file_crc;                          // 0x0C
    uint32                                   preferred_address;                 // 0x12
    Features                                 features;                          // 0x16

    uint32                                   generated_on;                      // 0x1A
    uint32                                   valid_from;                        // 0x1E
    uint32                                   valid_until;                       // 0x22

    array<CalendarOverrideEntry, uint8>      calendar_overrides;                // 0x26
    array<CalendarSchoolHolidayEntry, uint8> calendar_school_holidays;          // 0x2B
    array<DayTypeEntry, uint8>               day_types;                         // 0x30
    array<RouteCategoryEntry, uint8>         route_categories;                  // 0x35
    array<RouteEntry, uint16>                routes;                            // 0x3A
    array<StopEntry, uint16>                 stops;                             // 0x40
    array<ShapeEntry, uint16>                shapes;                            // 0x46
};

struct CalendarOverrideEntry // (0x0E - 14 bytes)
{
    uint8                                    id;                                // 0x00
    date                                     from_date;                         // 0x01
    date                                     to_date;                           // 0x05
    uint8                                    new_type;                          // 0x09
    char                                    *description;                       // 0x0A
};

struct CalendarSchoolHolidayEntry // (0x0D - 13 bytes)
{
    uint8                                    id;                                // 0x00
    date                                     from_date;                         // 0x01
    date                                     to_date;                           // 0x05
    char                                    *description;                       // 0x09
};

struct DayTypeEntry // (0x09 - 9 bytes)
{
    uint8                                    id;                                // 0x00
    char                                    *name;                              // 0x01
    char                                    *rule;                              // 0x05
};

struct RouteCategoryEntry // (0x13 - 19 bytes)
{
    uint8                                    id;                                // 0x00
    char                                    *name;                              // 0x01
    uint32                                   color;                             // 0x05
    uint32                                   flags;                             // 0x09

    array<RouteEntry, uint16>                routes;                            // 0x0D
};

struct RouteEntry // (0x1A - 26 bytes)
{
    uint16                                   id;                                // 0x00
    char                                    *name;                              // 0x01
    char                                    *long_name;                         // 0x05
    char                                    *description;                       // 0x09
    uint32                                   color;                             // 0x0D

    RouteCategoryEntry                      *category;                          // 0x11

    array<RouteLineEntry, uint8>             lines;                             // 0x15
};

struct RouteLineEntry // (0x22 - 34 bytes)
{
    RouteEntry                              *route;                             // 0x00
    uint8                                    direction;                         // 0x04
    char                                    *name;                              // 0x05
    uint8                                    time_id_logic;                     // 0x09
    RouteLineEntry                          *continuation;                      // 0x0A

    array<RouteHeadsignEntry, uint8>         headsigns;                         // 0x0E
    array<RouteStopEntry, uint8>             stops;                             // 0x13
    array<RouteTravelTimeEntry, uint8>       travel_times;                      // 0x18
    array<RouteDepartureGroupEntry, uint8>   departure_groups;                  // 0x1D
};

struct RouteHeadsignEntry // (0x05 - 5 bytes)
{
    uint8                                    stop_index;                        // 0x00
    char                                    *name;                              // 0x01
};

struct RouteStopEntry // (0x04 - 4 bytes)
{
    uint16                                   stop_id;                           // 0x00
    uint16                                   distance_traveled;                 // 0x02
};

struct RouteTravelTimeEntry // (0x08 - 8 bytes)
{
    uint16                                  *arrival_times;                     // 0x00
    uint16                                  *departure_times;                   // 0x00
};

struct RouteDepartureGroupEntry // (0x09 - 9 bytes)
{
    uint8                                    day_type_id;                       // 0x00
    uint8                                    time_id;                           // 0x01
    uint8                                    flags;                             // 0x02
    array<uint16, uint16>                    time_offsets;                      // 0x03
};

struct LocationEntry // (0x08 - 8 bytes)
{
    int32                                    latitude;                          // 0x00
    int32                                    longitude;                         // 0x04
};

struct StopEntry // (0x19 - 25 bytes)
{
    uint16                                   id;                                // 0x00
    uint16                                   group_id;                          // 0x02
    char                                    *name;                              // 0x04
    char                                    *subname;                           // 0x08
    char                                    *street;                            // 0x0C
    LocationEntry                            location;                          // 0x10
    uint8                                    flags;                             // 0x18
};

struct ShapeEntry // (0x09 - 9 bytes)
{
    uint16                                   first_stop_id;                     // 0x00
    uint16                                   next_stop_id;                      // 0x02
    array<LocationEntry, uint8>              points;                            // 0x04
};

#pragma pack(pop)

#endif
