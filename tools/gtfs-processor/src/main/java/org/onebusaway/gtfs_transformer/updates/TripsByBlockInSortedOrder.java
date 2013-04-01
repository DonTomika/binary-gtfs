/**
 * Copyright (C) 2011 Brian Ferris <bdferris@onebusaway.org>
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripsByBlockInSortedOrder {

  private static Logger _log = LoggerFactory.getLogger(TripsByBlockInSortedOrder.class);

  public static Map<String, List<Trip>> getTripsByBlockInSortedOrder(
      GtfsMutableRelationalDao dao) {

    Map<String, List<Trip>> tripsByBlockId = new HashMap<String, List<Trip>>();
    Map<Trip, Integer> averageStopTimeByTrip = new HashMap<Trip, Integer>();

    int totalTrips = 0;
    int tripsWithoutStopTimes = 0;

    for (Trip trip : dao.getAllTrips()) {

      totalTrips++;

      String blockId = trip.getBlockId();

      // Generate a random block id if none is present so we get no collisions
      if (blockId == null)
        blockId = trip.getId() + "-" + Math.random();

      List<Trip> trips = tripsByBlockId.get(blockId);
      if (trips == null) {
        trips = new ArrayList<Trip>();
        tripsByBlockId.put(blockId, trips);
      }
      trips.add(trip);

      List<StopTime> stopTimes = dao.getStopTimesForTrip(trip);
      if (stopTimes.isEmpty()) {
        tripsWithoutStopTimes++;
      } else {

        int arrivalTimes = 0;
        int arrivalTimeCount = 0;

        for (StopTime stopTime : stopTimes) {
          if (stopTime.isArrivalTimeSet()) {
            arrivalTimes += stopTime.getArrivalTime();
            arrivalTimeCount++;
          }
        }

        if (arrivalTimeCount > 0) {
          int averageArrivalTime = arrivalTimes / arrivalTimeCount;
          averageStopTimeByTrip.put(trip, averageArrivalTime);
        }
      }
    }

    _log.info("trips=" + totalTrips + " withoutStopTimes="
        + tripsWithoutStopTimes);

    TripComparator c = new TripComparator(averageStopTimeByTrip);

    for (List<Trip> tripsInBlock : tripsByBlockId.values()) {
      Collections.sort(tripsInBlock, c);
    }
    return tripsByBlockId;
  }

  private static class TripComparator implements Comparator<Trip> {

    private Map<Trip, Integer> _averageArrivalTimesByTrip;

    public TripComparator(Map<Trip, Integer> averageStopTimeByTrip) {
      _averageArrivalTimesByTrip = averageStopTimeByTrip;
    }

    @Override
    public int compare(Trip o1, Trip o2) {
      Integer st1 = _averageArrivalTimesByTrip.get(o1);
      Integer st2 = _averageArrivalTimesByTrip.get(o2);
      if (st1 == null && st2 == null)
        return o1.getId().compareTo(o2.getId());
      else if (st1 == null)
        return 1;
      else if (st2 == null)
        return -1;
      else
        return st1.compareTo(st2);
    }
  }

}
