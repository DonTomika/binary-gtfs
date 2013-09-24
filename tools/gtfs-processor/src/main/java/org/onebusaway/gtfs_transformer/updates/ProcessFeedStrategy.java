/**
 * Copyright (C) 2011 Brian Ferris <bdferris@onebusaway.org>
 * 
 * Copyright (C) 2013 Tamás Szincsák <dontomika@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onebusaway.gtfs_transformer.updates;

import java.awt.Color;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.onebusaway.collections.FactoryMap;
import org.onebusaway.csv_entities.schema.annotations.CsvField;
import org.onebusaway.gtfs.model.Agency;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Route;
import org.onebusaway.gtfs.model.ServiceCalendar;
import org.onebusaway.gtfs.model.ServiceCalendarDate;
import org.onebusaway.gtfs.model.ShapePoint;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.model.calendar.ServiceDate;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;
import org.onebusaway.gtfs_transformer.services.GtfsTransformStrategy;
import org.onebusaway.gtfs_transformer.services.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessFeedStrategy implements GtfsTransformStrategy
{
	@CsvField(ignore = true)
	private Logger _log = LoggerFactory.getLogger(ProcessFeedStrategy.class);
	
	@CsvField(ignore = true)
	private Map<Route, Integer> route_to_id = new HashMap<Route, Integer>();
	
	@CsvField(ignore = true)
	private Map<Stop, Integer> stop_to_id = new HashMap<Stop, Integer>();
	
	@CsvField(ignore = true)
	private Map<ServiceCalendar, Integer> calendar_to_id = new HashMap<ServiceCalendar, Integer>();
	
	@CsvField(ignore = true)
	private PrintStream out;
	
	@CsvField(ignore = true)
	private boolean is_dkv = false;
	
	@CsvField(ignore = true)
	private boolean is_bkv = false;
	
	public ProcessFeedStrategy(File outfile)
	{
		try
		{
			this.out = new PrintStream(outfile, "UTF-8");
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch (UnsupportedEncodingException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void run(TransformContext context, GtfsMutableRelationalDao dao)
	{
		
		Iterator<Agency> itr = dao.getAllAgencies().iterator();
		while (itr.hasNext())
		{
			Agency agency = itr.next();
			
			if (agency.getId().contains("DKV"))
				is_dkv = true;
			
			if (agency.getId().contains("BKK"))
				is_bkv = true;
		}
		
		if (is_bkv)
		{
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MONTH, 1);
			ServiceDate cutoff = new ServiceDate(cal);
			
			// trim stuff
			/*for (ServiceCalendar calendar : dao.getAllCalendars())
			{
				if (calendar.getStartDate().compareTo(cutoff) > 0)
				{
					for (ServiceCalendarDate date : dao.getCalendarDatesForServiceId(calendar.getServiceId());
					
					dao.removeEntity(calendar);
					
					
				}
			}*/
			
			UpdateLibrary.clearDaoCache(dao);
		}
		
		Map<CalendarPattern, List<ServiceCalendar>> calendarsToMerge = new FactoryMap<CalendarPattern, List<ServiceCalendar>>(new ArrayList<ServiceCalendar>());
		
		for (ServiceCalendar calendar : dao.getAllCalendars())
		{
			//ServiceCalendar calendar = dao.getCalendarForServiceId(trip.getServiceId());
			
			int monday = calendar.getMonday();
			int tuesday = calendar.getTuesday();
			int wednesday = calendar.getWednesday();
			int thursday = calendar.getThursday();
			int friday = calendar.getFriday();
			int saturday = calendar.getSaturday();
			int sunday = calendar.getSunday();
			ServiceDate start_date = calendar.getStartDate();
			ServiceDate end_date = calendar.getEndDate();
			
			List<ServiceCalendarDate> exceptions = dao.getCalendarDatesForServiceId(calendar.getServiceId());
			int n = exceptions.size();
			
			ServiceDate[] exception_dates = new ServiceDate[n];
			int[] exception_types = new int[n];
			
			for (int i = 0; i < n; i++)
			{
				ServiceCalendarDate exception = exceptions.get(i);
				
				exception_dates[i] = exception.getDate();
				exception_types[i] = exception.getExceptionType();
			}
			
			// sort
			for (int i = 0; i < n; i++)
			{
				for (int j = 1; j < n - i; j++)
				{
					if (exception_dates[j - 1].getAsDate().getTime() > exception_dates[j].getAsDate().getTime())
					{
						ServiceDate temp = exception_dates[j - 1];
						exception_dates[j - 1] = exception_dates[j];
						exception_dates[j] = temp;
						
						int temp2 = exception_types[j - 1];
						exception_types[j - 1] = exception_types[j];
						exception_types[j] = temp2;
					}
				}
			}
			
			CalendarPattern pattern = new CalendarPattern(monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date, exception_dates, exception_types);
			calendarsToMerge.get(pattern).add(calendar);
		}
		
		System.out.println("There are " + dao.getAllCalendars().size() + " calendars, merging them to " + calendarsToMerge.size());
		
		for (List<ServiceCalendar> merge : calendarsToMerge.values())
		{
			if (merge.size() == 1)
				continue;
			
			ServiceCalendar first = merge.get(0);
			
			for (int i = 1; i < merge.size(); i++)
			{
				ServiceCalendar next = merge.get(i);
				
				List<Trip> trips = dao.getTripsForServiceId(next.getServiceId());
				
				for (Trip trip : trips)
					trip.setServiceId(first.getServiceId());
				
				List<ServiceCalendarDate> calendar_dates = dao.getCalendarDatesForServiceId(next.getServiceId());
				
				for (ServiceCalendarDate calendar_date : calendar_dates)
					dao.removeEntity(calendar_date);
				
				dao.removeEntity(next);
			}
		}
		
		UpdateLibrary.clearDaoCache(dao);
		
		int counter = 0;
		
		// simplify calendars
		if (is_dkv)
		{
			for (ServiceCalendar calendar : dao.getAllCalendars())
			{
				List<ServiceCalendarDate> exceptions = dao.getCalendarDatesForServiceId(calendar.getServiceId());
				int days_yes[] = new int[7];
				int days_not[] = new int[7];
				
				ServiceDate start = calendar.getStartDate();
				do
				{
					int dow = start.getAsCalendar(TimeZone.getDefault()).get(Calendar.DAY_OF_WEEK);
					
					boolean valid = false;
					if (dow == Calendar.SUNDAY && calendar.getSunday() > 0)
						valid = true;
					else if (dow == Calendar.MONDAY && calendar.getMonday() > 0)
						valid = true;
					else if (dow == Calendar.TUESDAY && calendar.getTuesday() > 0)
						valid = true;
					else if (dow == Calendar.WEDNESDAY && calendar.getWednesday() > 0)
						valid = true;
					else if (dow == Calendar.THURSDAY && calendar.getThursday() > 0)
						valid = true;
					else if (dow == Calendar.FRIDAY && calendar.getFriday() > 0)
						valid = true;
					else if (dow == Calendar.SATURDAY && calendar.getSaturday() > 0)
						valid = true;
					
					for (ServiceCalendarDate exception : exceptions)
					{
						if (!exception.getDate().equals(start))
							continue;
						
						if (exception.getExceptionType() == 1)
							valid = true;
						else if (exception.getExceptionType() == 2)
							valid = false;
					}
					
					if (valid)
						days_yes[dow - 1]++;
					else
						days_not[dow - 1]++;
					
					start = start.next();
				} while (start.compareTo(calendar.getEndDate()) <= 0);
				
				for (int dow = 1; dow <= 7; dow++)
				{
					System.out.println("Calendar " + calendar.getServiceId() + ", dow: " + dow + ", yes: " + days_yes[dow - 1] + ", not: " + days_not[dow - 1]);
					
					if (days_yes[dow - 1] > days_not[dow - 1])
					{
						switch (dow)
						{
							case Calendar.SUNDAY:
								calendar.setSunday(1);
								break;
							case Calendar.MONDAY:
								calendar.setMonday(1);
								break;
							case Calendar.TUESDAY:
								calendar.setTuesday(1);
								break;
							case Calendar.WEDNESDAY:
								calendar.setWednesday(1);
								break;
							case Calendar.THURSDAY:
								calendar.setThursday(1);
								break;
							case Calendar.FRIDAY:
								calendar.setFriday(1);
								break;
							case Calendar.SATURDAY:
								calendar.setSaturday(1);
								break;
						}
						
						// add removes
						start = calendar.getStartDate();
						do
						{
							int dow2 = start.getAsCalendar(TimeZone.getDefault()).get(Calendar.DAY_OF_WEEK);
							
							boolean found = false;
							for (ServiceCalendarDate exception : exceptions)
							{
								if (exception.getDate().equals(start) && exception.getExceptionType() == 1)
									found = true;
							}
							
							if (dow2 == dow && !found)
							{
								ServiceCalendarDate temp = new ServiceCalendarDate();
								temp.setId(1000000 + ++counter);
								temp.setServiceId(calendar.getServiceId());
								temp.setDate(start);
								temp.setExceptionType(2);
								
								//exceptions.add(temp);
								dao.saveEntity(temp);
							}
							
							start = start.next();
						} while (start.compareTo(calendar.getEndDate()) <= 0);
						
						// remove adds
						Iterator<ServiceCalendarDate> itr2 = exceptions.iterator();
						while (itr2.hasNext())
						{
							ServiceCalendarDate exception = itr2.next();
							
							int dow2 = exception.getDate().getAsCalendar(TimeZone.getDefault()).get(Calendar.DAY_OF_WEEK);
							
							if (dow2 == dow && exception.getExceptionType() == 1)
							{
								//itr2.remove();
								dao.removeEntity(exception);
							}
						}
					}
				}
			}
		}
		
		UpdateLibrary.clearDaoCache(dao);
		
		// remove unused block ids
		for (Trip trip : dao.getAllTrips())
		{
			if (trip.getBlockId() == null)
				continue;
			
			List<Trip> trips = dao.getTripsForBlockId(new AgencyAndId(trip.getId().getAgencyId(), trip.getBlockId()));
			
			if (trips.size() < 2)
			{
				trip.setBlockId(null);
			}
		}
		
		UpdateLibrary.clearDaoCache(dao);
		
		// merge trips to lines
		
		Map<LinePattern, List<Trip>> tripsByLines = new FactoryMap<LinePattern, List<Trip>>(new ArrayList<Trip>());

		for (Trip trip : dao.getAllTrips())
		{
			List<StopTime> stopTimes = dao.getStopTimesForTrip(trip);
			
			System.out.println("Number of lines: " + tripsByLines.size());			

			int n = stopTimes.size();
			AgencyAndId[] stopIds = new AgencyAndId[n];
			
			for (int i = 0; i < n; i++)
			{
				StopTime stopTime = stopTimes.get(i);
				stopIds[i] = stopTime.getStop().getId();
			}
			
			LinePattern pattern = new LinePattern(trip.getRoute().getId(), trip.getDirectionId(), stopIds);
			tripsByLines.get(pattern).add(trip);
		}
		
		System.out.println("Number of lines: " + tripsByLines.size());
		
		int max_stop_times = 0;

		int index = 0;
		for (List<Trip> trips : tripsByLines.values())
		{
			Map<TravelTimePattern, List<Trip>> tripsByTravelTimes = new FactoryMap<TravelTimePattern, List<Trip>>(new ArrayList<Trip>());
			
			for (int i = 1; i < trips.size(); i++)
			{
				Trip trip = trips.get(i);
				List<StopTime> stopTimes = dao.getStopTimesForTrip(trip);
				
				int n = stopTimes.size();
				//AgencyAndId[] stopIds = new AgencyAndId[n];
				int first_arrival = 0;
				int first_departure = 0;
				int[] arrivalTimes = new int[n];
			    int[] departureTimes = new int[n];
				for (int j = 0; j < n; j++)
				{
					StopTime stopTime = stopTimes.get(j);
					//stopIds[j] = stopTime.getStop().getId();
					
					if (j == 0)
					{
						first_arrival = stopTime.getArrivalTime();
						first_departure = stopTime.getDepartureTime();
					}
					
					arrivalTimes[j] = stopTime.getArrivalTime() - first_arrival;
					departureTimes[j] = stopTime.getDepartureTime() - first_departure;
			    }
				
				TravelTimePattern pattern = new TravelTimePattern(arrivalTimes, departureTimes);
				tripsByTravelTimes.get(pattern).add(trip);
			}
			
			System.out.println(" - line #" + ++index + " has " + tripsByTravelTimes.size() + " different stop times for " + trips.size() + " trips");
			
			for (TravelTimePattern a : tripsByTravelTimes.keySet())
			{
				System.out.println("    - " + Arrays.toString(a._arrivalTimes));
			}
			
			if (max_stop_times < tripsByTravelTimes.size())
				max_stop_times = tripsByTravelTimes.size();
		}
		
		System.out.println("The maximum number of different stop times: " + max_stop_times);
		
		try
		{
			generateBaseForSQLite(dao);
			
			generateDayTypes(dao);
			
			generateRoutes(dao);
			
			generateStops(dao);
			
			generateRouteLines(dao, tripsByLines);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	private void generateBaseForMySQL(GtfsMutableRelationalDao dao) throws IOException
	{
		out.println("CREATE TABLE `metadata` (");
		out.println("  `content_version` int(11) NOT NULL,");
		out.println("  `valid_from` int(11) NOT NULL,");
		out.println("  `valid_until` int(11) NOT NULL");
		out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_hungarian_ci;");
		out.println("");
		
		out.println("CREATE TABLE `calendar_override` (");
		out.println("  `id` int(11) NOT NULL,");
		out.println("  `from_date` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `to_date` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `new_type` int(11) NOT NULL,");
		out.println("  `description` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  PRIMARY KEY (`id`)");
		out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_hungarian_ci;");
		out.println("");
		
		out.println("CREATE TABLE `calendar_school_holidays` (");
		out.println("  `id` int(11) NOT NULL,");
		out.println("  `from_date` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `to_date` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `description` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  PRIMARY KEY (`id`)");
		out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_hungarian_ci;");
		out.println("");
		
		out.println("CREATE TABLE `day_types` (");
		out.println("  `id` int(11) NOT NULL,");
		out.println("  `name` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `notes` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `rule` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  PRIMARY KEY (`id`)");
		out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_hungarian_ci;");
		out.println("");
		
		out.println("CREATE TABLE `route_categories` (");
		out.println("  `id` int(11) NOT NULL,");
		out.println("  `name` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `color` VARCHAR(8) NOT NULL DEFAULT 'FFFFFFFF',");
		out.println("  `flags` int(11) NOT NULL,");
		out.println("  PRIMARY KEY (`id`)");
		out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_hungarian_ci;");
		out.println("");
		
		out.println("CREATE TABLE `route_departures` (");
		out.println("  `route_id` int(11) DEFAULT NULL,");
		out.println("  `line_id` int(11) DEFAULT NULL,");
		out.println("  `day_type` int(11) DEFAULT NULL,");
		out.println("  `flags` int(11) DEFAULT NULL,");
		out.println("  `time_offset` int(11) DEFAULT NULL,");
		out.println("  `time_id` int(11) DEFAULT NULL");
		out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_hungarian_ci;");
		out.println("");
		
		out.println("CREATE TABLE `route_headsigns` (");
		out.println("  `route_id` int(11) NOT NULL,");
		out.println("  `line_id` int(11) NOT NULL,");
		out.println("  `stop_index` int(11) NOT NULL,");
		out.println("  `headsign` longtext COLLATE utf8_hungarian_ci NOT NULL");
		out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_hungarian_ci;");
		out.println("");
		
		out.println("CREATE TABLE `route_lines` (");
		out.println("  `route_id` int(11) NOT NULL,");
		out.println("  `line_id` int(11) NOT NULL,");
		out.println("  `direction` int(11) NOT NULL,");
		out.println("  `name` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `stop_times_count` int(11) NOT NULL");
		out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_hungarian_ci;");
		out.println("");
		
		out.println("CREATE TABLE `route_stops` (");
		out.println("  `route_id` int(11) NOT NULL,");
		out.println("  `line_id` int(11) NOT NULL,");
		out.println("  `stop_index` int(11) NOT NULL,");
		out.println("  `stop_id` int(11) NOT NULL,");
		out.println("  `distance_traveled` int(11) NOT NULL,");
		out.println("  `next_segment` longtext NOT NULL DEFAULT '',");
		for (int i = 1; i < 100; i++)
		{
			out.println("  `arrival_time_" + i + "` int(11) DEFAULT NULL,");
		}
		out.println("  `arrival_time_100` int(11) DEFAULT NULL,");
		for (int i = 1; i < 100; i++)
		{
			out.println("  `departure_time_" + i + "` int(11) DEFAULT NULL,");
		}
		out.println("  `departure_time_100` int(11) DEFAULT NULL");
		out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_hungarian_ci;");
		out.println("");
		
		out.println("CREATE TABLE `routes` (");
		out.println("  `id` int(11) NOT NULL,");
		out.println("  `name` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `long_name` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `description` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `color` VARCHAR(8) NOT NULL DEFAULT 'FFFFFFFF',");
		out.println("  `category_id` int(11) NOT NULL,");
		out.println("  PRIMARY KEY (`id`)");
		out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_hungarian_ci;");
		out.println("");
		
		out.println("CREATE TABLE `stops` (");
		out.println("  `id` int(11) NOT NULL,");
		out.println("  `name` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `subname` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `street` longtext COLLATE utf8_hungarian_ci NOT NULL,");
		out.println("  `group_id` int(11) NOT NULL,");
		out.println("  `latitude` int(11) NOT NULL,");
		out.println("  `longitude` int(11) NOT NULL,");
		out.println("  `flags` int(11) UNSIGNED NOT NULL,");
		out.println("  PRIMARY KEY (`id`)");
		out.println(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_hungarian_ci;");
		out.println("");
		
		long valid_from = Long.MAX_VALUE;
		long valid_until = 0;
		
		for (ServiceCalendar cal : dao.getAllCalendars())
		{
			if (valid_from > cal.getStartDate().getAsDate().getTime())
				valid_from = cal.getStartDate().getAsDate().getTime();
			
			if (valid_until < cal.getEndDate().getAsDate().getTime())
				valid_until = cal.getEndDate().getAsDate().getTime();
		}
		
		out.println("INSERT INTO `metadata` VALUES (1, " + (valid_from / 1000) + ", " + (valid_until / 1000 + 86399) + ");");
		
		/*out.println("INSERT INTO `calendar_override` VALUES ('1', '2012-01-01', '2012-01-01', '6', 'Újév');");
		out.println("INSERT INTO `calendar_override` VALUES ('2', '2012-03-15', '2012-03-15', '6', 'Nemzeti ünnep');");
		out.println("INSERT INTO `calendar_override` VALUES ('3', '2012-03-15', '2012-03-16', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('4', '2012-03-24', '2012-03-24', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('5', '2012-04-08', '2012-04-09', '6', 'Húsvét');");
		out.println("INSERT INTO `calendar_override` VALUES ('6', '2012-04-21', '2012-04-21', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('7', '2012-04-30', '2012-04-30', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('8', '2012-05-01', '2012-05-01', '6', 'Munka ünnepe');");
		out.println("INSERT INTO `calendar_override` VALUES ('9', '2012-05-27', '2012-05-28', '6', 'Pünkösd');");
		out.println("INSERT INTO `calendar_override` VALUES ('10', '2012-08-20', '2012-08-20', '6', 'Államalapítás ünnepe');");
		out.println("INSERT INTO `calendar_override` VALUES ('11', '2012-10-22', '2012-10-22', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('12', '2012-10-23', '2012-10-23', '6', 'Nemzeti ünnep');");
		out.println("INSERT INTO `calendar_override` VALUES ('13', '2012-10-27', '2012-10-27', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('14', '2012-11-01', '2012-11-01', '6', 'Mindenszentek');");
		out.println("INSERT INTO `calendar_override` VALUES ('15', '2012-11-02', '2012-11-02', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('16', '2012-11-10', '2012-11-10', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('17', '2012-12-15', '2012-12-15', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('18', '2012-12-24', '2012-12-24', '5', 'Pihenőnap (Karácsony)');");
		out.println("INSERT INTO `calendar_override` VALUES ('19', '2012-12-25', '2012-12-26', '6', 'Karácsony');");
		out.println("INSERT INTO `calendar_override` VALUES ('20', '2012-12-31', '2012-12-31', '5', 'Pihenőnap (Szilveszter)');");
		out.println("INSERT INTO `calendar_override` VALUES ('21', '2013-01-01', '2013-01-01', '6', 'Újév');");
		out.println("INSERT INTO `calendar_override` VALUES ('22', '2012-03-15', '2012-03-15', '6', 'Nemzeti ünnep');");
		out.println("");
		
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('1', '2012-04-05', '2012-04-09', 'Tavaszi szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('2', '2012-06-16', '2012-09-02', 'Nyári szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('3', '2012-10-29', '2012-11-04', 'Őszi szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('4', '2012-12-22', '2013-01-02', 'Téli szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('5', '2013-03-28', '2013-04-02', 'Tavaszi szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('6', '2013-06-15', '2013-08-31', 'Nyári szünet');");*/
		
		out.println("INSERT INTO `calendar_override` VALUES ('1', '2013-01-01', '2013-01-01', '6', 'Újév');");
		out.println("INSERT INTO `calendar_override` VALUES ('2', '2013-03-15', '2013-03-15', '6', 'Nemzeti ünnep');");
		out.println("INSERT INTO `calendar_override` VALUES ('3', '2013-03-31', '2013-04-01', '6', 'Húsvét');");
		out.println("INSERT INTO `calendar_override` VALUES ('4', '2013-05-01', '2013-05-01', '6', 'Munka ünnepe');");
		out.println("INSERT INTO `calendar_override` VALUES ('5', '2013-05-19', '2013-05-20', '6', 'Pünkösd');");
		out.println("INSERT INTO `calendar_override` VALUES ('6', '2013-08-19', '2013-08-19', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('7', '2013-08-20', '2013-08-20', '6', 'Államalapítás ünnepe');");
		out.println("INSERT INTO `calendar_override` VALUES ('8', '2013-08-24', '2013-08-24', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('9', '2013-10-23', '2013-10-23', '6', 'Nemzeti ünnep');");
		out.println("INSERT INTO `calendar_override` VALUES ('10', '2013-11-01', '2013-11-01', '6', 'Mindenszentek');");
		out.println("INSERT INTO `calendar_override` VALUES ('11', '2013-12-07', '2013-12-07', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('12', '2013-12-21', '2013-12-21', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('13', '2013-12-24', '2013-12-24', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('14', '2013-12-25', '2013-12-26', '6', 'Karácsony');");
		out.println("INSERT INTO `calendar_override` VALUES ('15', '2013-12-27', '2013-12-27', '5', 'Pihenőnap');");
		out.println("");

		out.println("INSERT INTO `calendar_school_holidays` VALUES ('1', '2012-12-22', '2013-01-02', 'Téli szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('2', '2013-03-28', '2013-04-02', 'Tavaszi szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('3', '2013-06-15', '2013-08-31', 'Nyári szünet');");
		out.println("");
		
		out.println("INSERT INTO `route_categories` VALUES ('1', 'Metró', 'E8E8E8', 1);");
		out.println("INSERT INTO `route_categories` VALUES ('2', 'Villamos', 'DBD229', 1);");
		out.println("INSERT INTO `route_categories` VALUES ('3', 'Trolibusz', 'E9473E', 1);");
		out.println("INSERT INTO `route_categories` VALUES ('4', 'Busz', '3E96D1', 1);");
		out.println("INSERT INTO `route_categories` VALUES ('5', 'Éjszakai', '999999', 1);");
		out.println("INSERT INTO `route_categories` VALUES ('6', 'Hév', 'E8E8E8', 1);");
		out.println("INSERT INTO `route_categories` VALUES ('7', 'Hajó', 'E8E8E8', 1);");
		out.println("");
		
		out.println("ALTER TABLE `route_lines`");
		out.println("  ADD PRIMARY KEY (`route_id`, `line_id`);");
		out.println("");
		
		out.println("ALTER TABLE `route_stops`");
		out.println("  ADD PRIMARY KEY (`route_id`, `line_id`, `stop_index`);");
		out.println("");
		
		out.println("ALTER TABLE `route_headsigns`");
		out.println("  ADD PRIMARY KEY (`route_id`, `line_id`, `stop_index`);");
		out.println("");
		
		out.println("ALTER TABLE `route_departures`");
		out.println("  ADD INDEX (`route_id`) ,");
		out.println("  ADD INDEX (`route_id`, `line_id`) ,");
		out.println("  ADD INDEX (`route_id`, `line_id`, `day_type`, `flags`);");
		out.println("");
	}
	
	private void generateBaseForSQLite(GtfsMutableRelationalDao dao) throws IOException
	{
		out.println("CREATE TABLE `calendar_override` (");
		out.println("	`id`  INTEGER(11) NOT NULL,");
		out.println("	`from_date`  TEXT NOT NULL,");
		out.println("	`to_date`  TEXT NOT NULL,");
		out.println("	`new_type`  INTEGER(11) NOT NULL,");
		out.println("	`description`  TEXT NOT NULL,");
		out.println("	PRIMARY KEY (`id`)");
		out.println(");");
		out.println("");
		out.println("CREATE TABLE `calendar_school_holidays` (");
		out.println("	`id`  INTEGER(11) NOT NULL,");
		out.println("	`from_date`  TEXT NOT NULL,");
		out.println("	`to_date`  TEXT NOT NULL,");
		out.println("	`description`  TEXT NOT NULL,");
		out.println("	PRIMARY KEY (`id`)");
		out.println(");");
		out.println("");
		out.println("CREATE TABLE `day_types` (");
		out.println("	`id`  INTEGER(11) NOT NULL,");
		out.println("	`name`  TEXT NOT NULL,");
		out.println("	`notes`  TEXT NOT NULL,");
		out.println("	`rule`  TEXT NOT NULL,");
		out.println("	PRIMARY KEY (`id`)");
		out.println(");");
		out.println("");
		out.println("CREATE TABLE `metadata` (");
		out.println("	`content_version`  INTEGER(11) NOT NULL,");
		out.println("	`valid_from`  INTEGER(11) NOT NULL,");
		out.println("	`valid_until`  INTEGER(11) NOT NULL");
		out.println(");");
		out.println("");
		out.println("CREATE TABLE `route_categories` (");
		out.println("	`id`  INTEGER(11) NOT NULL,");
		out.println("	`name`  TEXT NOT NULL,");
		out.println("	`color`  TEXT(8) NOT NULL,");
		out.println("	`flags`  INTEGER(11) NOT NULL,");
		out.println("	PRIMARY KEY (`id`)");
		out.println(");");
		out.println("");
		out.println("CREATE TABLE `route_departures` (");
		out.println("	`route_id`  INTEGER(11),");
		out.println("	`line_id`  INTEGER(11),");
		out.println("	`day_type`  INTEGER(11),");
		out.println("	`flags`  INTEGER(11),");
		out.println("	`time_offset`  INTEGER(11),");
		out.println("	`time_id`  INTEGER(11)");
		out.println(");");
		out.println("");
		out.println("CREATE TABLE `route_headsigns` (");
		out.println("	`route_id`  INTEGER(11) NOT NULL,");
		out.println("	`line_id`  INTEGER(11) NOT NULL,");
		out.println("	`stop_index`  INTEGER(11) NOT NULL,");
		out.println("	`headsign`  TEXT NOT NULL,");
		out.println("	PRIMARY KEY (`route_id`, `line_id`, `stop_index`)");
		out.println(");");
		out.println("");
		out.println("CREATE TABLE `route_lines` (");
		out.println("	`route_id`  INTEGER(11) NOT NULL,");
		out.println("	`line_id`  INTEGER(11) NOT NULL,");
		out.println("	`direction`  INTEGER(11) NOT NULL,");
		out.println("	`name`  TEXT NOT NULL,");
		out.println("	`stop_times_count`  INTEGER(11) NOT NULL,");
		out.println("	`flags`  INTEGER(11) NOT NULL,");
		out.println("	PRIMARY KEY (`route_id`, `line_id`)");
		out.println(");");
		out.println("");
		out.println("CREATE TABLE `route_stops` (");
		out.println("	`route_id`  INTEGER(11) NOT NULL,");
		out.println("	`line_id`  INTEGER(11) NOT NULL,");
		out.println("	`stop_index`  INTEGER(11) NOT NULL,");
		out.println("	`stop_id`  INTEGER(11) NOT NULL,");
		out.println("	`distance_traveled`  INTEGER(11) NOT NULL,");
		out.println("	`next_segment`  TEXT NOT NULL,");
		out.println("	`arrival_time_1`  INTEGER(11),");
		out.println("	`arrival_time_2`  INTEGER(11),");
		out.println("	`arrival_time_3`  INTEGER(11),");
		out.println("	`arrival_time_4`  INTEGER(11),");
		out.println("	`arrival_time_5`  INTEGER(11),");
		out.println("	`arrival_time_6`  INTEGER(11),");
		out.println("	`arrival_time_7`  INTEGER(11),");
		out.println("	`arrival_time_8`  INTEGER(11),");
		out.println("	`arrival_time_9`  INTEGER(11),");
		out.println("	`arrival_time_10`  INTEGER(11),");
		out.println("	`arrival_time_11`  INTEGER(11),");
		out.println("	`arrival_time_12`  INTEGER(11),");
		out.println("	`arrival_time_13`  INTEGER(11),");
		out.println("	`arrival_time_14`  INTEGER(11),");
		out.println("	`arrival_time_15`  INTEGER(11),");
		out.println("	`arrival_time_16`  INTEGER(11),");
		out.println("	`arrival_time_17`  INTEGER(11),");
		out.println("	`arrival_time_18`  INTEGER(11),");
		out.println("	`arrival_time_19`  INTEGER(11),");
		out.println("	`arrival_time_20`  INTEGER(11),");
		out.println("	`arrival_time_21`  INTEGER(11),");
		out.println("	`arrival_time_22`  INTEGER(11),");
		out.println("	`arrival_time_23`  INTEGER(11),");
		out.println("	`arrival_time_24`  INTEGER(11),");
		out.println("	`arrival_time_25`  INTEGER(11),");
		out.println("	`arrival_time_26`  INTEGER(11),");
		out.println("	`arrival_time_27`  INTEGER(11),");
		out.println("	`arrival_time_28`  INTEGER(11),");
		out.println("	`arrival_time_29`  INTEGER(11),");
		out.println("	`arrival_time_30`  INTEGER(11),");
		out.println("	`arrival_time_31`  INTEGER(11),");
		out.println("	`arrival_time_32`  INTEGER(11),");
		out.println("	`arrival_time_33`  INTEGER(11),");
		out.println("	`arrival_time_34`  INTEGER(11),");
		out.println("	`arrival_time_35`  INTEGER(11),");
		out.println("	`arrival_time_36`  INTEGER(11),");
		out.println("	`arrival_time_37`  INTEGER(11),");
		out.println("	`arrival_time_38`  INTEGER(11),");
		out.println("	`arrival_time_39`  INTEGER(11),");
		out.println("	`arrival_time_40`  INTEGER(11),");
		out.println("	`arrival_time_41`  INTEGER(11),");
		out.println("	`arrival_time_42`  INTEGER(11),");
		out.println("	`arrival_time_43`  INTEGER(11),");
		out.println("	`arrival_time_44`  INTEGER(11),");
		out.println("	`arrival_time_45`  INTEGER(11),");
		out.println("	`arrival_time_46`  INTEGER(11),");
		out.println("	`arrival_time_47`  INTEGER(11),");
		out.println("	`arrival_time_48`  INTEGER(11),");
		out.println("	`arrival_time_49`  INTEGER(11),");
		out.println("	`arrival_time_50`  INTEGER(11),");
		out.println("	`arrival_time_51`  INTEGER(11),");
		out.println("	`arrival_time_52`  INTEGER(11),");
		out.println("	`arrival_time_53`  INTEGER(11),");
		out.println("	`arrival_time_54`  INTEGER(11),");
		out.println("	`arrival_time_55`  INTEGER(11),");
		out.println("	`arrival_time_56`  INTEGER(11),");
		out.println("	`arrival_time_57`  INTEGER(11),");
		out.println("	`arrival_time_58`  INTEGER(11),");
		out.println("	`arrival_time_59`  INTEGER(11),");
		out.println("	`arrival_time_60`  INTEGER(11),");
		out.println("	`arrival_time_61`  INTEGER(11),");
		out.println("	`arrival_time_62`  INTEGER(11),");
		out.println("	`arrival_time_63`  INTEGER(11),");
		out.println("	`arrival_time_64`  INTEGER(11),");
		out.println("	`arrival_time_65`  INTEGER(11),");
		out.println("	`arrival_time_66`  INTEGER(11),");
		out.println("	`arrival_time_67`  INTEGER(11),");
		out.println("	`arrival_time_68`  INTEGER(11),");
		out.println("	`arrival_time_69`  INTEGER(11),");
		out.println("	`arrival_time_70`  INTEGER(11),");
		out.println("	`arrival_time_71`  INTEGER(11),");
		out.println("	`arrival_time_72`  INTEGER(11),");
		out.println("	`arrival_time_73`  INTEGER(11),");
		out.println("	`arrival_time_74`  INTEGER(11),");
		out.println("	`arrival_time_75`  INTEGER(11),");
		out.println("	`arrival_time_76`  INTEGER(11),");
		out.println("	`arrival_time_77`  INTEGER(11),");
		out.println("	`arrival_time_78`  INTEGER(11),");
		out.println("	`arrival_time_79`  INTEGER(11),");
		out.println("	`arrival_time_80`  INTEGER(11),");
		out.println("	`arrival_time_81`  INTEGER(11),");
		out.println("	`arrival_time_82`  INTEGER(11),");
		out.println("	`arrival_time_83`  INTEGER(11),");
		out.println("	`arrival_time_84`  INTEGER(11),");
		out.println("	`arrival_time_85`  INTEGER(11),");
		out.println("	`arrival_time_86`  INTEGER(11),");
		out.println("	`arrival_time_87`  INTEGER(11),");
		out.println("	`arrival_time_88`  INTEGER(11),");
		out.println("	`arrival_time_89`  INTEGER(11),");
		out.println("	`arrival_time_90`  INTEGER(11),");
		out.println("	`arrival_time_91`  INTEGER(11),");
		out.println("	`arrival_time_92`  INTEGER(11),");
		out.println("	`arrival_time_93`  INTEGER(11),");
		out.println("	`arrival_time_94`  INTEGER(11),");
		out.println("	`arrival_time_95`  INTEGER(11),");
		out.println("	`arrival_time_96`  INTEGER(11),");
		out.println("	`arrival_time_97`  INTEGER(11),");
		out.println("	`arrival_time_98`  INTEGER(11),");
		out.println("	`arrival_time_99`  INTEGER(11),");
		out.println("	`arrival_time_100`  INTEGER(11),");
		out.println("	`departure_time_1`  INTEGER(11),");
		out.println("	`departure_time_2`  INTEGER(11),");
		out.println("	`departure_time_3`  INTEGER(11),");
		out.println("	`departure_time_4`  INTEGER(11),");
		out.println("	`departure_time_5`  INTEGER(11),");
		out.println("	`departure_time_6`  INTEGER(11),");
		out.println("	`departure_time_7`  INTEGER(11),");
		out.println("	`departure_time_8`  INTEGER(11),");
		out.println("	`departure_time_9`  INTEGER(11),");
		out.println("	`departure_time_10`  INTEGER(11),");
		out.println("	`departure_time_11`  INTEGER(11),");
		out.println("	`departure_time_12`  INTEGER(11),");
		out.println("	`departure_time_13`  INTEGER(11),");
		out.println("	`departure_time_14`  INTEGER(11),");
		out.println("	`departure_time_15`  INTEGER(11),");
		out.println("	`departure_time_16`  INTEGER(11),");
		out.println("	`departure_time_17`  INTEGER(11),");
		out.println("	`departure_time_18`  INTEGER(11),");
		out.println("	`departure_time_19`  INTEGER(11),");
		out.println("	`departure_time_20`  INTEGER(11),");
		out.println("	`departure_time_21`  INTEGER(11),");
		out.println("	`departure_time_22`  INTEGER(11),");
		out.println("	`departure_time_23`  INTEGER(11),");
		out.println("	`departure_time_24`  INTEGER(11),");
		out.println("	`departure_time_25`  INTEGER(11),");
		out.println("	`departure_time_26`  INTEGER(11),");
		out.println("	`departure_time_27`  INTEGER(11),");
		out.println("	`departure_time_28`  INTEGER(11),");
		out.println("	`departure_time_29`  INTEGER(11),");
		out.println("	`departure_time_30`  INTEGER(11),");
		out.println("	`departure_time_31`  INTEGER(11),");
		out.println("	`departure_time_32`  INTEGER(11),");
		out.println("	`departure_time_33`  INTEGER(11),");
		out.println("	`departure_time_34`  INTEGER(11),");
		out.println("	`departure_time_35`  INTEGER(11),");
		out.println("	`departure_time_36`  INTEGER(11),");
		out.println("	`departure_time_37`  INTEGER(11),");
		out.println("	`departure_time_38`  INTEGER(11),");
		out.println("	`departure_time_39`  INTEGER(11),");
		out.println("	`departure_time_40`  INTEGER(11),");
		out.println("	`departure_time_41`  INTEGER(11),");
		out.println("	`departure_time_42`  INTEGER(11),");
		out.println("	`departure_time_43`  INTEGER(11),");
		out.println("	`departure_time_44`  INTEGER(11),");
		out.println("	`departure_time_45`  INTEGER(11),");
		out.println("	`departure_time_46`  INTEGER(11),");
		out.println("	`departure_time_47`  INTEGER(11),");
		out.println("	`departure_time_48`  INTEGER(11),");
		out.println("	`departure_time_49`  INTEGER(11),");
		out.println("	`departure_time_50`  INTEGER(11),");
		out.println("	`departure_time_51`  INTEGER(11),");
		out.println("	`departure_time_52`  INTEGER(11),");
		out.println("	`departure_time_53`  INTEGER(11),");
		out.println("	`departure_time_54`  INTEGER(11),");
		out.println("	`departure_time_55`  INTEGER(11),");
		out.println("	`departure_time_56`  INTEGER(11),");
		out.println("	`departure_time_57`  INTEGER(11),");
		out.println("	`departure_time_58`  INTEGER(11),");
		out.println("	`departure_time_59`  INTEGER(11),");
		out.println("	`departure_time_60`  INTEGER(11),");
		out.println("	`departure_time_61`  INTEGER(11),");
		out.println("	`departure_time_62`  INTEGER(11),");
		out.println("	`departure_time_63`  INTEGER(11),");
		out.println("	`departure_time_64`  INTEGER(11),");
		out.println("	`departure_time_65`  INTEGER(11),");
		out.println("	`departure_time_66`  INTEGER(11),");
		out.println("	`departure_time_67`  INTEGER(11),");
		out.println("	`departure_time_68`  INTEGER(11),");
		out.println("	`departure_time_69`  INTEGER(11),");
		out.println("	`departure_time_70`  INTEGER(11),");
		out.println("	`departure_time_71`  INTEGER(11),");
		out.println("	`departure_time_72`  INTEGER(11),");
		out.println("	`departure_time_73`  INTEGER(11),");
		out.println("	`departure_time_74`  INTEGER(11),");
		out.println("	`departure_time_75`  INTEGER(11),");
		out.println("	`departure_time_76`  INTEGER(11),");
		out.println("	`departure_time_77`  INTEGER(11),");
		out.println("	`departure_time_78`  INTEGER(11),");
		out.println("	`departure_time_79`  INTEGER(11),");
		out.println("	`departure_time_80`  INTEGER(11),");
		out.println("	`departure_time_81`  INTEGER(11),");
		out.println("	`departure_time_82`  INTEGER(11),");
		out.println("	`departure_time_83`  INTEGER(11),");
		out.println("	`departure_time_84`  INTEGER(11),");
		out.println("	`departure_time_85`  INTEGER(11),");
		out.println("	`departure_time_86`  INTEGER(11),");
		out.println("	`departure_time_87`  INTEGER(11),");
		out.println("	`departure_time_88`  INTEGER(11),");
		out.println("	`departure_time_89`  INTEGER(11),");
		out.println("	`departure_time_90`  INTEGER(11),");
		out.println("	`departure_time_91`  INTEGER(11),");
		out.println("	`departure_time_92`  INTEGER(11),");
		out.println("	`departure_time_93`  INTEGER(11),");
		out.println("	`departure_time_94`  INTEGER(11),");
		out.println("	`departure_time_95`  INTEGER(11),");
		out.println("	`departure_time_96`  INTEGER(11),");
		out.println("	`departure_time_97`  INTEGER(11),");
		out.println("	`departure_time_98`  INTEGER(11),");
		out.println("	`departure_time_99`  INTEGER(11),");
		out.println("	`departure_time_100`  INTEGER(11),");
		out.println("	PRIMARY KEY (`route_id`, `line_id`, `stop_index`)");
		out.println(");");
		out.println("");
		out.println("CREATE TABLE `routes` (");
		out.println("	`id`  INTEGER(11) NOT NULL,");
		out.println("	`name`  TEXT NOT NULL,");
		out.println("	`long_name`  TEXT NOT NULL,");
		out.println("	`description`  TEXT NOT NULL,");
		out.println("	`color`  TEXT(8) NOT NULL,");
		out.println("	`category_id`  INTEGER(11) NOT NULL,");
		out.println("	PRIMARY KEY (`id`)");
		out.println(");");
		out.println("");
		out.println("CREATE TABLE `stops` (");
		out.println("	`id`  INTEGER(11) NOT NULL,");
		out.println("	`name`  TEXT NOT NULL,");
		out.println("	`subname`  TEXT NOT NULL,");
		out.println("	`street`  TEXT NOT NULL,");
		out.println("	`group_id`  INTEGER(11) NOT NULL,");
		out.println("	`latitude`  INTEGER(11) NOT NULL,");
		out.println("	`longitude`  INTEGER(11) NOT NULL,");
		out.println("	`flags`  INTEGER(11) NOT NULL,");
		out.println("	PRIMARY KEY (`id`)");
		out.println(");");
		
		long valid_from = Long.MAX_VALUE;
		long valid_until = 0;
		
		for (ServiceCalendar cal : dao.getAllCalendars())
		{
			if (valid_from > cal.getStartDate().getAsDate().getTime())
				valid_from = cal.getStartDate().getAsDate().getTime();
			
			if (valid_until < cal.getEndDate().getAsDate().getTime())
				valid_until = cal.getEndDate().getAsDate().getTime();
		}
		
		out.println("INSERT INTO `metadata` VALUES (1, " + (valid_from / 1000) + ", " + (valid_until / 1000 + 86399) + ");");
		
		/*out.println("INSERT INTO `calendar_override` VALUES ('1', '2012-01-01', '2012-01-01', '6', 'Újév');");
		out.println("INSERT INTO `calendar_override` VALUES ('2', '2012-03-15', '2012-03-15', '6', 'Nemzeti ünnep');");
		out.println("INSERT INTO `calendar_override` VALUES ('3', '2012-03-15', '2012-03-16', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('4', '2012-03-24', '2012-03-24', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('5', '2012-04-08', '2012-04-09', '6', 'Húsvét');");
		out.println("INSERT INTO `calendar_override` VALUES ('6', '2012-04-21', '2012-04-21', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('7', '2012-04-30', '2012-04-30', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('8', '2012-05-01', '2012-05-01', '6', 'Munka ünnepe');");
		out.println("INSERT INTO `calendar_override` VALUES ('9', '2012-05-27', '2012-05-28', '6', 'Pünkösd');");
		out.println("INSERT INTO `calendar_override` VALUES ('10', '2012-08-20', '2012-08-20', '6', 'Államalapítás ünnepe');");
		out.println("INSERT INTO `calendar_override` VALUES ('11', '2012-10-22', '2012-10-22', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('12', '2012-10-23', '2012-10-23', '6', 'Nemzeti ünnep');");
		out.println("INSERT INTO `calendar_override` VALUES ('13', '2012-10-27', '2012-10-27', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('14', '2012-11-01', '2012-11-01', '6', 'Mindenszentek');");
		out.println("INSERT INTO `calendar_override` VALUES ('15', '2012-11-02', '2012-11-02', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('16', '2012-11-10', '2012-11-10', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('17', '2012-12-15', '2012-12-15', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('18', '2012-12-24', '2012-12-24', '5', 'Pihenőnap (Karácsony)');");
		out.println("INSERT INTO `calendar_override` VALUES ('19', '2012-12-25', '2012-12-26', '6', 'Karácsony');");
		out.println("INSERT INTO `calendar_override` VALUES ('20', '2012-12-31', '2012-12-31', '5', 'Pihenőnap (Szilveszter)');");
		out.println("INSERT INTO `calendar_override` VALUES ('21', '2013-01-01', '2013-01-01', '6', 'Újév');");
		out.println("INSERT INTO `calendar_override` VALUES ('22', '2012-03-15', '2012-03-15', '6', 'Nemzeti ünnep');");
		out.println("");
		
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('1', '2012-04-05', '2012-04-09', 'Tavaszi szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('2', '2012-06-16', '2012-09-02', 'Nyári szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('3', '2012-10-29', '2012-11-04', 'Őszi szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('4', '2012-12-22', '2013-01-02', 'Téli szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('5', '2013-03-28', '2013-04-02', 'Tavaszi szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('6', '2013-06-15', '2013-08-31', 'Nyári szünet');");*/
		
		out.println("INSERT INTO `calendar_override` VALUES ('1', '2013-01-01', '2013-01-01', '6', 'Újév');");
		out.println("INSERT INTO `calendar_override` VALUES ('2', '2013-03-15', '2013-03-15', '6', 'Nemzeti ünnep');");
		out.println("INSERT INTO `calendar_override` VALUES ('3', '2013-03-31', '2013-04-01', '6', 'Húsvét');");
		out.println("INSERT INTO `calendar_override` VALUES ('4', '2013-05-01', '2013-05-01', '6', 'Munka ünnepe');");
		out.println("INSERT INTO `calendar_override` VALUES ('5', '2013-05-19', '2013-05-20', '6', 'Pünkösd');");
		out.println("INSERT INTO `calendar_override` VALUES ('6', '2013-08-19', '2013-08-19', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('7', '2013-08-20', '2013-08-20', '6', 'Államalapítás ünnepe');");
		out.println("INSERT INTO `calendar_override` VALUES ('8', '2013-08-24', '2013-08-24', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('9', '2013-10-23', '2013-10-23', '6', 'Nemzeti ünnep');");
		out.println("INSERT INTO `calendar_override` VALUES ('10', '2013-11-01', '2013-11-01', '6', 'Mindenszentek');");
		out.println("INSERT INTO `calendar_override` VALUES ('11', '2013-12-07', '2013-12-07', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('12', '2013-12-21', '2013-12-21', '0', 'Munkanap');");
		out.println("INSERT INTO `calendar_override` VALUES ('13', '2013-12-24', '2013-12-24', '5', 'Pihenőnap');");
		out.println("INSERT INTO `calendar_override` VALUES ('14', '2013-12-25', '2013-12-26', '6', 'Karácsony');");
		out.println("INSERT INTO `calendar_override` VALUES ('15', '2013-12-27', '2013-12-27', '5', 'Pihenőnap');");
		out.println("");

		out.println("INSERT INTO `calendar_school_holidays` VALUES ('1', '2012-12-22', '2013-01-02', 'Téli szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('2', '2013-03-28', '2013-04-02', 'Tavaszi szünet');");
		out.println("INSERT INTO `calendar_school_holidays` VALUES ('3', '2013-06-15', '2013-08-31', 'Nyári szünet');");
		out.println("");
		
		if (is_dkv)
		{
			out.println("INSERT INTO `route_categories` VALUES ('1', 'Villamos', 'DBD229', 1);");
			out.println("INSERT INTO `route_categories` VALUES ('2', 'Trolibusz', 'E9473E', 1);");
			out.println("INSERT INTO `route_categories` VALUES ('3', 'Busz', '3E96D1', 1);");
			out.println("INSERT INTO `route_categories` VALUES ('4', 'Áruházi járatok', 'D07F66', 0);");
			out.println("INSERT INTO `route_categories` VALUES ('5', 'Egyéb', '808080', 0);");
		}
		else
		{
			out.println("INSERT INTO `route_categories` VALUES ('1', 'Metró', 'E8E8E8', 1);");
			out.println("INSERT INTO `route_categories` VALUES ('2', 'Villamos', 'DBD229', 1);");
			out.println("INSERT INTO `route_categories` VALUES ('3', 'Trolibusz', 'E9473E', 1);");
			out.println("INSERT INTO `route_categories` VALUES ('4', 'Busz', '3E96D1', 1);");
			out.println("INSERT INTO `route_categories` VALUES ('5', 'Éjszakai', '999999', 1);");
			out.println("INSERT INTO `route_categories` VALUES ('6', 'Hév', 'E8E8E8', 1);");
			out.println("INSERT INTO `route_categories` VALUES ('7', 'Hajó', 'E8E8E8', 1);");
		}
		
		out.println("");
		
		out.println("CREATE INDEX rd1 ON `route_departures` (`route_id`);");
		out.println("CREATE INDEX rd1 ON `route_departures` (`route_id`, `line_id`);");
		out.println("CREATE INDEX rd1 ON `route_departures` (`route_id`, `line_id`, `day_type`, `flags`);");
		out.println("");
	}
	
	private void generateDayTypes(GtfsMutableRelationalDao dao) throws IOException
	{
		int index = 0;
		for (ServiceCalendar calendar : dao.getAllCalendars())
		{
			int monday = calendar.getMonday();
			int tuesday = calendar.getTuesday();
			int wednesday = calendar.getWednesday();
			int thursday = calendar.getThursday();
			int friday = calendar.getFriday();
			int saturday = calendar.getSaturday();
			int sunday = calendar.getSunday();
			ServiceDate start_date = calendar.getStartDate();
			ServiceDate end_date = calendar.getEndDate();
			
			List<ServiceCalendarDate> exceptions = dao.getCalendarDatesForServiceId(calendar.getServiceId());
			int n = exceptions.size();
			
			ServiceDate[] exception_dates = new ServiceDate[n];
			int[] exception_types = new int[n];
			
			for (int i = 0; i < n; i++)
			{
				ServiceCalendarDate exception = exceptions.get(i);
				
				exception_dates[i] = exception.getDate();
				exception_types[i] = exception.getExceptionType();
			}
			
			String rule = "";
			
			rule += "e>" + (start_date.getYear() * 10000 + start_date.getMonth() * 100 + start_date.getDay() - 1) + ",";
			rule += "e<" + (end_date.getYear() * 10000 + end_date.getMonth() * 100 + end_date.getDay() + 1) + ",";
			
			for (int i = 0; i < n; i++)
			{
				if (exception_types[i] == 2)
					rule += "!e=" + (exception_dates[i].getYear() * 10000 + exception_dates[i].getMonth() * 100 + exception_dates[i].getDay()) + ",";
			}
			
			int counter = 0;

			for (int i = 0; i < n; i++)
				if (exception_types[i] == 1)
					counter++;
			
			counter += monday + tuesday + wednesday + thursday + friday + saturday + sunday;
			
			for (int i = 0; i < counter - 1; i++)
				rule += "+,";
			
			if (monday == 1) rule += "r=0,";
			if (tuesday == 1) rule += "r=1,";
			if (wednesday == 1) rule += "r=2,";
			if (thursday == 1) rule += "r=3,";
			if (friday == 1) rule += "r=4,";
			if (saturday == 1) rule += "r=5,";
			if (sunday == 1) rule += "r=6,";
			
			for (int i = 0; i < n; i++)
			{
				if (exception_types[i] == 1)
					rule += "e=" + (exception_dates[i].getYear() * 10000 + exception_dates[i].getMonth() * 100 + exception_dates[i].getDay()) + ",";
			}
			
			rule = rule.substring(0, rule.length() - 1);
			
			out.println(String.format(
					"INSERT INTO day_types VALUES (%s, \"%s\", \"%s\", \"%s\");",
					new Object[] { Integer.toString(++index), "DayType " + index, "no notes", rule }
				));
			
			calendar_to_id.put(calendar, index);
		}
	}

	private void generateStops(GtfsMutableRelationalDao dao) throws IOException
	{
		Map<String, Integer> group_by_station = new HashMap<String, Integer>();
		
		Map<String, Integer> group_by_name = new HashMap<String, Integer>();
		
		Map<String, Integer> group_by_stop_id = new HashMap<String, Integer>();
		
		int group_counter = 0;
		int index = 0;
		for (Stop stop : dao.getAllStops())
		{
			if (stop.getLocationType() == 1)
				continue;
			
			// group_id
			int group_id = 0;
			
			if (stop.getParentStation() != null)
			{
				if (group_by_station.containsKey(stop.getParentStation()))
					group_id = group_by_station.get(stop.getParentStation());
				else
					group_by_station.put(stop.getParentStation(), group_id = ++group_counter);
			}
			else if (is_dkv)
			{
				String gid = stop.getId().getId().substring(0, 5);
				
				if (group_by_name.containsKey(gid))
					group_id = group_by_name.get(gid);
				else
					group_by_name.put(gid, group_id = ++group_counter);
			}
			else
			{
				if (group_by_name.containsKey(stop.getName()))
					group_id = group_by_name.get(stop.getName());
				else
					group_by_name.put(stop.getName(), group_id = ++group_counter);
			}
			
			// wheelchair_boarding
			int flags = 0;
			
			if (stop.getParentStation() != null)
			{
				Stop station = dao.getStopForId(new AgencyAndId(stop.getId().getAgencyId(), stop.getParentStation()));
				
				if (station.getWheelchairBoarding() == 1)
					flags = 3;
				else if (station.getWheelchairBoarding() == 2)
					flags = 1;
			}
			
			if (stop.getWheelchairBoarding() == 1)
				flags = 3;
			else if (stop.getWheelchairBoarding() == 2)
				flags = 1;
				
			out.println(String.format(
					"INSERT INTO stops VALUES (%s, %s, %s, %s, %s, %s, %s, %s);",
					new Object[] { Integer.toString(++index), getEscapedString(stop.getName()), getEscapedString(stop.getDesc()), getEscapedString(stop.getStreet()), Integer.toString(group_id), Integer.toString((int) (stop.getLat() * 1000000.0 + 0.5)), Integer.toString((int) (stop.getLon() * 1000000.0 + 0.5)), Integer.toString(flags) }
				));
			
			stop_to_id.put(stop, index);
		}
	}
	
	private Object getEscapedString(String str)
	{
		if (str == null)
			return "\"\"";
		else
			return '"' + str.replace('"', '\"') + '"';
	}

	private void generateRoutes(GtfsMutableRelationalDao dao) throws IOException
	{
		final Map<Route, Integer> route_to_category_id = new HashMap<Route, Integer>();

		for (Route route : dao.getAllRoutes())
		{
			int category_id = 0;
			
			if (route.getShortName() == null)
				route.setShortName("UNK");
			
			if (is_dkv)
			{
				switch (route.getType())
				{
					case 0:
						category_id = 1;
						break;
					case 3:
						if (route.getAgency().getId().contains("HAJDU-VOLAN") || route.getAgency().getId().contains("TESCO"))
							category_id = 4; // auchan/tesco
						else if (route.getShortName().equals("A1") || route.getShortName().startsWith("MUZ") || route.getAgency().getId().contains("KOMAROMI"))
							category_id = 5; // egyéb
						else if (route.getColor() != null && route.getColor().equals("FF1818"))
							category_id = 2; // trolibusz
						else
							category_id = 3; // busz
						break;
					case 800:
						category_id = 2;
						break;
				}
			}
			else
			{
				switch (route.getType())
				{
					case 0:
						category_id = 2;
						break;
					case 1:
						category_id = 1;
						break;
					case 2:
						category_id = 6;
						break;
					case 3:
						if (route.getColor() != null && route.getColor().equals("333333"))
							category_id = 5;
						else if (route.getColor() != null && route.getColor().equals("FF1818"))
							category_id = 3;
						else
							category_id = 4;
						break;
					case 4:
						category_id = 7;
						break;
				}
			}
			
			route_to_category_id.put(route,  category_id);
		}
		
		List<Route> sorted_routes = new ArrayList<Route>();
		sorted_routes.addAll(dao.getAllRoutes());
		
		Collections.sort(sorted_routes, new Comparator<Route>() {
			@Override
			public int compare(Route o1, Route o2)
			{
				int category_id_1 = route_to_category_id.get(o1);
				int category_id_2 = route_to_category_id.get(o2);
				
				if (category_id_1 != category_id_2)
					return Integer.compare(category_id_1, category_id_2);
				
				NaturalOrderComparator comp = new NaturalOrderComparator();
				return comp.compare(o1.getShortName(), o2.getShortName());
			}
		});
		
		// rename duplicate routes
		Map<String, Route> route_names = new HashMap<String, Route>();
		Map<String, Integer> route_name_counter = new HashMap<String, Integer>();
		
		for (Route r : sorted_routes)
		{
			if (route_names.containsKey(r.getShortName() + ":" + r.getType()))
			{
				String basename = r.getShortName();
				
				int counter = route_name_counter.get(basename + ":" + r.getType());
				
				if (counter == 1)
				{
					Route first = route_names.get(r.getShortName() + ":" + r.getType());
					first.setShortName(r.getShortName() + "\u00B9");
					r.setShortName(r.getShortName() + "\u00B2");
				}
				else if (counter == 2)
					r.setShortName(r.getShortName() + "\u00B3");
				else if (counter == 3)
					r.setShortName(r.getShortName() + "\u2074");
				else if (counter == 4)
					r.setShortName(r.getShortName() + "\u2075");
				else if (counter == 5)
					r.setShortName(r.getShortName() + "\u2076");
				else if (counter == 6)
					r.setShortName(r.getShortName() + "\u2077");
				else if (counter == 7)
					r.setShortName(r.getShortName() + "\u2078");
				else if (counter == 8)
					r.setShortName(r.getShortName() + "\u2079");
				
				route_name_counter.put(basename + ":" + r.getType(), counter + 1);
			}
			else
			{
				route_names.put(r.getShortName() + ":" + r.getType(), r);
				route_name_counter.put(r.getShortName() + ":" + r.getType(), 1);
			}
		}
		
		int index = 0;
		for (Route route : sorted_routes)
		{
			int route_id = ++index;
			int category_id = route_to_category_id.get(route);
			String color = transformColor(route.getColor());
			String short_name = route.getShortName();
			String long_name = route.getDesc();
			String desc = route.getLongName();
			
			// hax for dkv
			if (is_dkv && (category_id == 4 || category_id == 5))
			{
				if (short_name.startsWith("AU"))
					color = transformColor("ED7175");
				else if (short_name.startsWith("T"))
					color = transformColor("61C1F4");
				else if (short_name.startsWith("A"))
					color = transformColor("ED69FF");
				else if (short_name.startsWith("GGY"))
					color = transformColor("999999");
			}
			
			if (is_dkv)
			{
				desc = "";
				long_name = long_name.replace(") (", ", ");
				
				// move comments in long_name to desc
				if (long_name != null && long_name.contains("(") && long_name.endsWith(")"))
				{
					String new_long_name = long_name.substring(0, long_name.lastIndexOf('('));
					String new_desc = long_name.substring(long_name.lastIndexOf('(') + 1, long_name.length() - 1).replace("IDEIGLENES", "ideiglenes").replace("GYORSJÁRAT", "gyorsjárat");
					
					if (desc == null || desc.isEmpty())
					{
						long_name = new_long_name;
						desc = new_desc;
					}
					else if (desc.equalsIgnoreCase(new_desc))
					{
						long_name = new_long_name;
					}
					else if (desc.toLowerCase().contains(new_desc.toLowerCase()))
					{
						long_name = new_long_name;
						desc = new_desc;
					}
					else
					{
						long_name = new_long_name;
						desc = desc + ", " + new_desc;
					}
				}
			}
			
			desc = desc == null ? null : desc.trim();
			long_name = long_name == null ? long_name : long_name.trim();
			
			out.println(String.format(
					"INSERT INTO routes VALUES (%s, %s, %s, %s, \"%s\", %s);",
					new Object[] { Integer.toString(route_id), getEscapedString(short_name), getEscapedString(long_name), getEscapedString(desc), color, Integer.toString(category_id) }
				));
			
			route_to_id.put(route, route_id);
		}
	}
	
	private String transformColor(String color)
	{
		if (color == null)
			return "FFFFFFFF";
		
		Color originalColour = new Color(Integer.parseInt(color, 16));
		
		float hsbVals[] = Color.RGBtoHSB(originalColour.getRed(), originalColour.getGreen(), originalColour.getBlue(), null);
		Color highlight = Color.getHSBColor(hsbVals[0], hsbVals[1], 0.5f * (1f + hsbVals[2]));
		//Color shadow = Color.getHSBColor(hsbVals[0], hsbVals[1], 0.5f * hsbVals[2]);
		
		return Integer.toHexString(highlight.getRGB());
	}

	private void generateRouteLines(GtfsMutableRelationalDao dao, Map<LinePattern, List<Trip>> tripsByLines) throws IOException
	{
		/*File output_lines = new File(out_dir, "route_lines.sql");
		output_lines.delete();
		PrintStream out_lines = new PrintStream(output_lines, "UTF-8");
		
		File output_headsigns = new File(out_dir, "route_headsigns.sql");
		output_headsigns.delete();
		PrintStream out_headsigns = new PrintStream(output_headsigns, "UTF-8");
		
		File output_stops = new File(out_dir, "route_stops.sql");
		output_stops.delete();
		PrintStream out_stops = new PrintStream(output_stops, "UTF-8");
		
		File output_departures = new File(out_dir, "route_departures.sql");
		output_departures.delete();
		PrintStream out_departures = new PrintStream(output_departures, "UTF-8");*/
		
		PrintStream out_lines = out;
		PrintStream out_headsigns = out;
		PrintStream out_stops = out;
		PrintStream out_departures = out;
		
		int index = 0;
		for (List<Trip> trips : tripsByLines.values())
		{
			Map<TravelTimePattern, List<Trip>> tripsByTravelTimes = new FactoryMap<TravelTimePattern, List<Trip>>(new ArrayList<Trip>());
			
			for (int i = 0; i < trips.size(); i++)
			{
				Trip trip = trips.get(i);
				List<StopTime> stopTimes = dao.getStopTimesForTrip(trip);
				
				int n = stopTimes.size();
				int first_arrival = 0;
				int first_departure = 0;
				int[] arrivalTimes = new int[n];
			    int[] departureTimes = new int[n];
				for (int j = 0; j < n; j++)
				{
					StopTime stopTime = stopTimes.get(j);
					
					if (j == 0)
					{
						first_arrival = stopTime.getArrivalTime();
						first_departure = stopTime.getDepartureTime();
					}
					
					arrivalTimes[j] = stopTime.getArrivalTime() - first_arrival;
					departureTimes[j] = stopTime.getDepartureTime() - first_departure;
			    }
				
				TravelTimePattern pattern = new TravelTimePattern(arrivalTimes, departureTimes);
				tripsByTravelTimes.get(pattern).add(trip);
			}
			
			Trip first = trips.get(0);
			
			List<StopTime> stopTimes = dao.getStopTimesForTrip(first);
			
			int line_flags = 0;
			if (first.getTripsBkkRef() == null || first.getTripsBkkRef().length() == 0)
				line_flags = 1;
			
			out_lines.println(String.format(
					"INSERT INTO route_lines VALUES (%s, %s, %s, %s, %s, %s);",
					new Object[] { Integer.toString(route_to_id.get(first.getRoute())), Integer.toString(++index), first.getDirectionId() != null ? first.getDirectionId() : "0", getEscapedString(first.getRoute().getShortName()), Integer.toString(tripsByTravelTimes.size()), Integer.toString(line_flags) }
				));
			
			out_headsigns.println(String.format(
					"INSERT INTO route_headsigns VALUES (%s, %s, %s, %s);",
					new Object[] { Integer.toString(route_to_id.get(first.getRoute())), Integer.toString(index), Integer.toString(0), getEscapedString(stopTimes.get(0).getStop().getName()) }
				));
			
			String last_headsign = stopTimes.get(0).getStop().getName();
			
			if (stopTimes.size() == 0 || stopTimes.get(0).getStopHeadsign() == null)
			{
				out_headsigns.println(String.format(
						"INSERT INTO route_headsigns VALUES (%s, %s, %s, %s);",
						new Object[] { Integer.toString(route_to_id.get(first.getRoute())), Integer.toString(index), Integer.toString(1), getEscapedString(first.getTripHeadsign()) }
					));
				
				last_headsign = first.getTripHeadsign();
			}

			Map<TravelTimePattern, Integer> time_index_to_id = new HashMap<TravelTimePattern, Integer>();
			
			int index2 = 0;
			for (int i = 0; i < stopTimes.size(); i++)
			{
				StopTime stopTime = stopTimes.get(i);
				
				if (stopTime.getStopHeadsign() != null && !stopTime.getStopHeadsign().equals(last_headsign))
				{
					out_headsigns.println(String.format(
							"INSERT INTO route_headsigns VALUES (%s, %s, %s, %s);",
							new Object[] { Integer.toString(route_to_id.get(first.getRoute())), Integer.toString(index), Integer.toString(i + 1), getEscapedString(stopTime.getStopHeadsign()) }
						));
					
					last_headsign = stopTime.getStopHeadsign();
				}

				String arrival_times = "";
				String departure_times = "";
				
				int counter = 0;
				for (TravelTimePattern key : tripsByTravelTimes.keySet())
				{
					time_index_to_id.put(key, counter + 1);

					arrival_times += key._arrivalTimes[index2] + ", ";
					departure_times += key._departureTimes[index2] + ", ";
					counter++;
				}
				
				for (; counter < 100; counter++)
				{
					arrival_times += "NULL, ";
					departure_times += "NULL, ";
				}
				
				arrival_times = arrival_times.substring(0, arrival_times.length() - 2);
				departure_times = departure_times.substring(0, departure_times.length() - 2);
				
				String shapes = "";
				
				List<ShapePoint> points = dao.getShapePointsForShapeId(first.getShapeId());
				
				if (stopTime.getShapeDistTraveled() >= 0)
				{
					// find out the correct segment using distance_traveled
					for (ShapePoint point : points)
					{
						double dist = point.getDistTraveled();
						
						if (dist < stopTime.getShapeDistTraveled())
							continue;
						
						/*if ((int) (dist * 10000) == (int) (stopTime.getShapeDistTraveled() * 10000) && Math.abs(point.getLat() - stopTime.getStop().getLat()) < 0.0000005 && Math.abs(point.getLon() - stopTime.getStop().getLon()) < 0.0000005)
							continue;*/
						
						if (Math.abs(point.getLat() - stopTime.getStop().getLat()) < 0.000001 && Math.abs(point.getLon() - stopTime.getStop().getLon()) < 0.0000001)
							continue;
						
						if (i < stopTimes.size() - 1 && Math.abs(point.getLat() - stopTimes.get(i + 1).getStop().getLat()) < 0.000001 && Math.abs(point.getLon() - stopTimes.get(i + 1).getStop().getLon()) < 0.0000001)
							continue;
						
						if (i < stopTimes.size() - 1 && dist >= stopTimes.get(i + 1).getShapeDistTraveled())
							break;
						
						shapes += (int) (point.getLat() * 1000000.0 + 0.5) + "," + (int) (point.getLon() * 1000000.0 + 0.5) + ",";
					}
				}
				else
				{
					// do the same with using only coordinates
					/*int start_index = 0;
					int end_index = 0;
					
					double best_dist = 999999999.0;
					for (int j = 0; j < points.size(); j++)
					{
						ShapePoint point = points.get(j);
						
						double dist = getDistance(point.getLat(), point.getLon(), stopTime.getStop().getLat(), stopTime.getStop().getLon());
						
						if (dist < best_dist)
						{
							start_index = j;
							best_dist = dist;
						}
					}
					
					if (i < stopTimes.size() - 1)
					{
						best_dist = 999999999.0;
						for (int j = 0; j < points.size(); j++)
						{
							ShapePoint point = points.get(j);
							
							double dist = getDistance(point.getLat(), point.getLon(), stopTimes.get(i + 1).getStop().getLat(), stopTimes.get(i + 1).getStop().getLon());
							
							if (dist < best_dist)
							{
								end_index = j;
								best_dist = dist;
							}
						}
					}
					else
						end_index = start_index - 1;
					
					for (int j = start_index; j <= end_index; j++)
					{
						ShapePoint point = points.get(j);
					
						if (Math.abs(point.getLat() - stopTime.getStop().getLat()) < 0.000001 && Math.abs(point.getLon() - stopTime.getStop().getLon()) < 0.0000001)
							continue;
						
						if (i < stopTimes.size() - 1 && Math.abs(point.getLat() - stopTimes.get(i + 1).getStop().getLat()) < 0.000001 && Math.abs(point.getLon() - stopTimes.get(i + 1).getStop().getLon()) < 0.0000001)
							continue;
						
						shapes += (int) (point.getLat() * 1000000.0 + 0.5) + "," + (int) (point.getLon() * 1000000.0 + 0.5) + ",";
					}*/
				}
				
				if (shapes.length() > 1)
					shapes = shapes.substring(0, shapes.length() - 1);
				
				int dist_traveled = (int) (stopTime.getShapeDistTraveled() + 0.5);
				
				out_stops.println(String.format(
						"INSERT INTO route_stops VALUES (%s, %s, %s, %s, %s, \"%s\", %s, %s);",
						new Object[] { Integer.toString(route_to_id.get(first.getRoute())), Integer.toString(index), Integer.toString(++index2), Integer.toString(stop_to_id.get(stopTime.getStop())), dist_traveled, shapes, arrival_times, departure_times }
					));
			}
			
			for (Entry<TravelTimePattern, List<Trip>> entry : tripsByTravelTimes.entrySet())
			{
				TravelTimePattern key = entry.getKey();
				List<Trip> subtrips = entry.getValue();
				
				for (Trip trip : subtrips)
				{
					List<StopTime> stopTimes2 = dao.getStopTimesForTrip(trip);
					
					if (stopTimes2.size() > 0)
					{
						int flags = trip.getWheelchairAccessible() > 0 ? trip.getWheelchairAccessible() == 1 ? 3 : 1 : 0;
						StopTime stopTime = stopTimes2.get(0);
						
						Integer route_id = route_to_id.get(trip.getRoute());
						
						if (route_id == null)
						{
							System.out.println("No route for trip: " + trip.getId());
							continue;
						}
					
						ServiceCalendar calendar = dao.getCalendarForServiceId(trip.getServiceId());
						
						if (calendar == null)
						{
							System.out.println("No calendar for trip: " + trip.getId() + ", service id: " + trip.getServiceId());
							continue;
						}
						
						out_departures.println(String.format(
								"INSERT INTO route_departures VALUES (%s, %s, %s, %s, %s, %s);",
								new Object[] {
										Integer.toString(route_id),
										Integer.toString(index),
										Integer.toString(calendar_to_id.get(calendar)),
										Integer.toString(flags),
										Integer.toString(stopTime.getDepartureTime()),
										Integer.toString(time_index_to_id.get(key)) }
								));
					}
				}
			}
		}
		
		/*out_lines.close();
		out_headsigns.close();
		out_stops.close();
		out_departures.close();*/
	}
	
	private double getDistance(double lat, double lon, double lat2, double lon2)
	{
		double d2r = Math.PI / 180;
	    double dlong = (lon2 - lon) * d2r;
	    double dlat = (lat2 - lat) * d2r;
	    double a =
	        Math.pow(Math.sin(dlat / 2.0), 2)
	            + Math.cos(lat * d2r)
	            * Math.cos(lat2 * d2r)
	            * Math.pow(Math.sin(dlong / 2.0), 2);
	    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
	    double d = 6367 * c;

	    return d;
	}

	private static class CalendarPattern
	{
		public final int _monday;
		public final int _tuesday;
		public final int _wednesday;
		public final int _thursday;
		public final int _friday;
		public final int _saturday;
		public final int _sunday;
		public final ServiceDate _start_date;
		public final ServiceDate _end_date;
		public final ServiceDate[] _exception_dates;
		public final int[] _exception_types;
		
		public CalendarPattern(int monday, int tuesday, int wednesday, int thursday, int friday, int saturday, int sunday, ServiceDate start_date, ServiceDate end_date, ServiceDate[] exception_dates, int[] exception_types)
		{
			this._monday = monday;
			this._tuesday = tuesday;
			this._wednesday = wednesday;
			this._thursday = thursday;
			this._friday = friday;
			this._saturday = saturday;
			this._sunday = sunday;
			this._start_date = start_date;
			this._end_date = end_date;
			this._exception_dates = exception_dates;
			this._exception_types = exception_types;
		}

		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((_end_date == null) ? 0 : _end_date.hashCode());
			result = prime * result + Arrays.hashCode(_exception_dates);
			result = prime * result + Arrays.hashCode(_exception_types);
			result = prime * result + _friday;
			result = prime * result + _monday;
			result = prime * result + _saturday;
			result = prime * result
					+ ((_start_date == null) ? 0 : _start_date.hashCode());
			result = prime * result + _sunday;
			result = prime * result + _thursday;
			result = prime * result + _tuesday;
			result = prime * result + _wednesday;
			return result;
		}

		@Override
		public boolean equals(Object obj)
		{
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CalendarPattern other = (CalendarPattern) obj;
			if (_end_date == null) {
				if (other._end_date != null)
					return false;
			} else if (!_end_date.equals(other._end_date))
				return false;
			if (!Arrays.equals(_exception_dates, other._exception_dates))
				return false;
			if (!Arrays.equals(_exception_types, other._exception_types))
				return false;
			if (_friday != other._friday)
				return false;
			if (_monday != other._monday)
				return false;
			if (_saturday != other._saturday)
				return false;
			if (_start_date == null) {
				if (other._start_date != null)
					return false;
			} else if (!_start_date.equals(other._start_date))
				return false;
			if (_sunday != other._sunday)
				return false;
			if (_thursday != other._thursday)
				return false;
			if (_tuesday != other._tuesday)
				return false;
			if (_wednesday != other._wednesday)
				return false;
			return true;
		}
	}

	private static class LinePattern
	{
		public final AgencyAndId _routeId;
		public final String _directionId;
		public final AgencyAndId[] _stopIds;

		public LinePattern(AgencyAndId routeId, String directionId, AgencyAndId[] stopIds)
		{
			_routeId = routeId;
			_directionId = directionId;
			_stopIds = stopIds;
		}

		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((_directionId == null) ? 0 : _directionId.hashCode());
			result = prime * result
					+ ((_routeId == null) ? 0 : _routeId.hashCode());
			result = prime * result + Arrays.hashCode(_stopIds);
			return result;
		}

		@Override
		public boolean equals(Object obj)
		{
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			LinePattern other = (LinePattern) obj;
			if (_directionId == null) {
				if (other._directionId != null)
					return false;
			} else if (!_directionId.equals(other._directionId))
				return false;
			if (_routeId == null) {
				if (other._routeId != null)
					return false;
			} else if (!_routeId.equals(other._routeId))
				return false;
			if (!Arrays.equals(_stopIds, other._stopIds))
				return false;
			return true;
		}
	}
	
	private static class TravelTimePattern
	{
		public final int[] _arrivalTimes;
		public final int[] _departureTimes;

		public TravelTimePattern(int[] arrivalTimes, int[] departureTimes)
		{
			_arrivalTimes = arrivalTimes;
			_departureTimes = departureTimes;
		}

		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + Arrays.hashCode(_arrivalTimes);
			result = prime * result + Arrays.hashCode(_departureTimes);
			return result;
		}

		@Override
		public boolean equals(Object obj)
		{
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TravelTimePattern other = (TravelTimePattern) obj;
			if (!Arrays.equals(_arrivalTimes, other._arrivalTimes))
				return false;
			if (!Arrays.equals(_departureTimes, other._departureTimes))
				return false;
			return true;
		}
	}
	
	/*
	 NaturalOrderComparator.java -- Perform 'natural order' comparisons of strings in Java.
	 Copyright (C) 2003 by Pierre-Luc Paour <natorder@paour.com>

	 Based on the C version by Martin Pool, of which this is more or less a straight conversion.
	 Copyright (C) 2000 by Martin Pool <mbp@humbug.org.au>

	 This software is provided 'as-is', without any express or implied
	 warranty.  In no event will the authors be held liable for any damages
	 arising from the use of this software.

	 Permission is granted to anyone to use this software for any purpose,
	 including commercial applications, and to alter it and redistribute it
	 freely, subject to the following restrictions:

	 1. The origin of this software must not be misrepresented; you must not
	 claim that you wrote the original software. If you use this software
	 in a product, an acknowledgment in the product documentation would be
	 appreciated but is not required.
	 2. Altered source versions must be plainly marked as such, and must not be
	 misrepresented as being the original software.
	 3. This notice may not be removed or altered from any source distribution.
	 */
	public static class NaturalOrderComparator implements Comparator
	{
	    int compareRight(String a, String b)
	    {
	        int bias = 0;
	        int ia = 0;
	        int ib = 0;

	        // The longest run of digits wins. That aside, the greatest
	        // value wins, but we can't know that it will until we've scanned
	        // both numbers to know that they have the same magnitude, so we
	        // remember it in BIAS.
	        for (;; ia++, ib++)
	        {
	            char ca = charAt(a, ia);
	            char cb = charAt(b, ib);

	            if (!Character.isDigit(ca) && !Character.isDigit(cb))
	            {
	                return bias;
	            }
	            else if (!Character.isDigit(ca))
	            {
	                return -1;
	            }
	            else if (!Character.isDigit(cb))
	            {
	                return +1;
	            }
	            else if (ca < cb)
	            {
	                if (bias == 0)
	                {
	                    bias = -1;
	                }
	            }
	            else if (ca > cb)
	            {
	                if (bias == 0)
	                    bias = +1;
	            }
	            else if (ca == 0 && cb == 0)
	            {
	                return bias;
	            }
	        }
	    }

	    public int compare(Object o1, Object o2)
	    {
	        String a = o1.toString();
	        String b = o2.toString();

	        int ia = 0, ib = 0;
	        int nza = 0, nzb = 0;
	        char ca, cb;
	        int result;

	        while (true)
	        {
	            // only count the number of zeroes leading the last number compared
	            nza = nzb = 0;

	            ca = charAt(a, ia);
	            cb = charAt(b, ib);

	            // skip over leading spaces or zeros
	            while (Character.isSpaceChar(ca) || ca == '0')
	            {
	                if (ca == '0')
	                {
	                    nza++;
	                }
	                else
	                {
	                    // only count consecutive zeroes
	                    nza = 0;
	                }

	                ca = charAt(a, ++ia);
	            }

	            while (Character.isSpaceChar(cb) || cb == '0')
	            {
	                if (cb == '0')
	                {
	                    nzb++;
	                }
	                else
	                {
	                    // only count consecutive zeroes
	                    nzb = 0;
	                }

	                cb = charAt(b, ++ib);
	            }

	            // process run of digits
	            if (Character.isDigit(ca) && Character.isDigit(cb))
	            {
	                if ((result = compareRight(a.substring(ia), b.substring(ib))) != 0)
	                {
	                    return result;
	                }
	            }

	            if (ca == 0 && cb == 0)
	            {
	                // The strings compare the same. Perhaps the caller
	                // will want to call strcmp to break the tie.
	                return nza - nzb;
	            }

	            if (ca < cb)
	            {
	                return -1;
	            }
	            else if (ca > cb)
	            {
	                return +1;
	            }

	            ++ia;
	            ++ib;
	        }
	    }

	    static char charAt(String s, int i)
	    {
	        if (i >= s.length())
	        {
	            return 0;
	        }
	        else
	        {
	            return s.charAt(i);
	        }
	    }
	}
}
