/**
 * Copyright (C) 2012 Google, Inc.
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
package org.onebusaway.gtfs_merge;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.onebusaway.gtfs.impl.GtfsRelationalDaoImpl;
import org.onebusaway.gtfs.serialization.GtfsReader;
import org.onebusaway.gtfs.serialization.GtfsWriter;
import org.onebusaway.gtfs_merge.strategies.AgencyMergeStrategy;
import org.onebusaway.gtfs_merge.strategies.EntityMergeStrategy;
import org.onebusaway.gtfs_merge.strategies.FareAttributeMergeStrategy;
import org.onebusaway.gtfs_merge.strategies.FareRuleMergeStrategy;
import org.onebusaway.gtfs_merge.strategies.FrequencyMergeStrategy;
import org.onebusaway.gtfs_merge.strategies.RouteMergeStrategy;
import org.onebusaway.gtfs_merge.strategies.ServiceCalendarMergeStrategy;
import org.onebusaway.gtfs_merge.strategies.ShapePointMergeStrategy;
import org.onebusaway.gtfs_merge.strategies.StopMergeStrategy;
import org.onebusaway.gtfs_merge.strategies.TransferMergeStrategy;
import org.onebusaway.gtfs_merge.strategies.TripMergeStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GtfsMerger {

  private static Logger _log = LoggerFactory.getLogger(GtfsMerger.class);

  private static final String _alphaPrefix = "abcdefghijklmnopqrstuvwxyz";

  private static final NumberFormat _numberPrefixFormat = new DecimalFormat(
      "00");

  private EntityMergeStrategy _agencyStrategy = new AgencyMergeStrategy();

  private EntityMergeStrategy _stopStrategy = new StopMergeStrategy();

  private EntityMergeStrategy _serviceCalendarStrategy = new ServiceCalendarMergeStrategy();

  private EntityMergeStrategy _routeStrategy = new RouteMergeStrategy();

  private EntityMergeStrategy _tripStrategy = new TripMergeStrategy();

  private EntityMergeStrategy _shapePointStrategy = new ShapePointMergeStrategy();

  private EntityMergeStrategy _frequencyStrategy = new FrequencyMergeStrategy();

  private EntityMergeStrategy _transferStrategy = new TransferMergeStrategy();

  private EntityMergeStrategy _fareAttributeStrategy = new FareAttributeMergeStrategy();

  private EntityMergeStrategy _fareRuleStrategy = new FareRuleMergeStrategy();

  public void setAgencyStrategy(EntityMergeStrategy agencyStrategy) {
    _agencyStrategy = agencyStrategy;
  }

  public void setStopStrategy(EntityMergeStrategy stopsStrategy) {
    _stopStrategy = stopsStrategy;
  }

  public void setServiceCalendarStrategy(
      EntityMergeStrategy serviceCalendarStrategy) {
    _serviceCalendarStrategy = serviceCalendarStrategy;
  }

  public void setRouteStrategy(EntityMergeStrategy routeStrategy) {
    _routeStrategy = routeStrategy;
  }

  public void setTripStrategy(EntityMergeStrategy tripStrategy) {
    _tripStrategy = tripStrategy;
  }

  public void setShapePointStrategy(EntityMergeStrategy shapePointStrategy) {
    _shapePointStrategy = shapePointStrategy;
  }

  public void setFrequencyStrategy(EntityMergeStrategy frequencyStrategy) {
    _frequencyStrategy = frequencyStrategy;
  }

  public void setTransferStrategy(EntityMergeStrategy transferStrategy) {
    _transferStrategy = transferStrategy;
  }

  public void setFareAttributeStrategy(EntityMergeStrategy fareAttributeStrategy) {
    _fareAttributeStrategy = fareAttributeStrategy;
  }

  public void setFareRuleStrategy(EntityMergeStrategy fareRuleStrategy) {
    _fareRuleStrategy = fareRuleStrategy;
  }

  public EntityMergeStrategy getEntityMergeStrategyForEntityType(
      Class<?> entityType) {
    List<EntityMergeStrategy> strategies = new ArrayList<EntityMergeStrategy>();
    buildStrategies(strategies);
    for (EntityMergeStrategy strategy : strategies) {
      Set<Class<?>> entityTypes = new HashSet<Class<?>>();
      strategy.getEntityTypes(entityTypes);
      if (entityTypes.contains(entityType)) {
        return strategy;
      }
    }
    return null;
  }

  public void run(List<File> inputPaths, File outputPath) throws IOException {

    GtfsRelationalDaoImpl mergedDao = new GtfsRelationalDaoImpl();
    mergedDao.setPackShapePoints(true);
    mergedDao.setPackStopTimes(true);

    List<EntityMergeStrategy> strategies = new ArrayList<EntityMergeStrategy>();
    buildStrategies(strategies);

    /**
     * For each entity merge strategy, we keep track of a mapping from raw GTFS
     * ids to entities, if the particular entity type has an identifier. This
     * will be used to detect id conflicts between subsequent runs of each merge
     * strategy on different feeds. We can't use the AgencyAndId ids in the DAO
     * because it might be possible for two entities with the same id but
     * different agency prefixes to sneak in. Since we ultimately serialize the
     * data to a GTFS feed with no agency prefixes, we need to track the raw id.
     */
    Map<EntityMergeStrategy, Map<String, Object>> rawEntityIdMapsByMergeStrategy = new HashMap<EntityMergeStrategy, Map<String, Object>>();
    for (EntityMergeStrategy strategy : strategies) {
      rawEntityIdMapsByMergeStrategy.put(strategy,
          new HashMap<String, Object>());
    }

    /**
     * We iterate over the input feeds in reverse order, such that entities from
     * the newest feeds are added first and older entities are potentially
     * dropped.
     */
    for (int index = inputPaths.size() - 1; index >= 0; --index) {
      File inputPath = inputPaths.get(index);
      String prefix = getIndexAsPrefix(index, inputPaths.size());

      _log.info("reading input: " + inputPath);

      GtfsReader reader = new GtfsReader();
      reader.setInputLocation(inputPath);

      GtfsRelationalDaoImpl dao = new GtfsRelationalDaoImpl();
      dao.setPackShapePoints(true);
      dao.setPackStopTimes(true);
      reader.setEntityStore(dao);
      reader.run();

      for (EntityMergeStrategy strategy : strategies) {
        _log.info("strategy=" + strategy.getClass());
        GtfsMergeContext context = new GtfsMergeContext(dao, mergedDao, prefix,
            rawEntityIdMapsByMergeStrategy.get(strategy));
        strategy.merge(context);
      }
    }

    _log.info("writing merged output: " + outputPath);

    GtfsWriter writer = new GtfsWriter();
    writer.setOutputLocation(outputPath);
    writer.run(mergedDao);
  }

  private String getIndexAsPrefix(int index, int total) {
    if (total <= _alphaPrefix.length()) {
      return Character.toString(_alphaPrefix.charAt(index)) + "-";
    }
    return _numberPrefixFormat.format(index) + "-";
  }

  private void buildStrategies(List<EntityMergeStrategy> strategies) {
    strategies.add(_agencyStrategy);
    strategies.add(_stopStrategy);
    strategies.add(_serviceCalendarStrategy);
    strategies.add(_routeStrategy);
    strategies.add(_tripStrategy);
    strategies.add(_shapePointStrategy);
    strategies.add(_frequencyStrategy);
    strategies.add(_transferStrategy);
    strategies.add(_fareAttributeStrategy);
    strategies.add(_fareRuleStrategy);
  }
}
