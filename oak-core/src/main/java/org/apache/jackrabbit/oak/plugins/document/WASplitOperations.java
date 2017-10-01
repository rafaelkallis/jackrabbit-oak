/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.filter;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.*;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.*;

/**
 * Utility class to create document split operations.
 */
class WASplitOperations extends SplitOperations {

    private static final Logger LOG = LoggerFactory.getLogger(WASplitOperations.class);

    private WASplitOperations(@Nonnull NodeDocument doc, @Nonnull RevisionContext context, @Nonnull RevisionVector headRev, @Nonnull Function<String, Long> binarySize, int numRevsThreshold) {
        super(doc, context, headRev, binarySize, numRevsThreshold);
    }


    @Override
    protected void collectLocalChanges(Map<String, NavigableMap<Revision, String>> committedLocally, Set<Revision> changes) {
        for (String property : filter(doc.keySet(), PROPERTY_OR_DELETED)) {
            NavigableMap<Revision, String> splitMap =
                    new TreeMap<Revision, String>(StableRevisionComparator.INSTANCE);
            committedLocally.put(property, splitMap);

            Map<Revision, String> valueMap = doc.getLocalMap(property);

            int vol = 0;
            boolean first = true;

            // collect committed changes of this cluster node
            for (Map.Entry<Revision, String> entry : valueMap.entrySet()) {
                Revision rev = entry.getKey();

                if (property.equals("_deleted")) {
                    if (first && rev.getClusterId() == context.getClusterId()) {
                        first = false;
                        if (isInSlidingWindow(rev)) {
                            ++vol;
                        }
                        continue;
                    }
                    if (isInSlidingWindow(rev) && isVisible(rev) && vol++ < context.getVolatilityThreshold()) {
                        continue;
                    }
                }
                if (rev.getClusterId() != context.getClusterId()) {
                    continue;
                }

                changes.add(rev);
                if (isCommitted(context.getCommitValue(rev, doc))) {
                    splitMap.put(rev, entry.getValue());
                } else if (isGarbage(rev)) {
                    addGarbage(rev, property);
                }
            }
        }
    }

    private boolean isVisible(Revision r){
        return r.getClusterId() == context.getClusterId()
                || (r.compareRevisionTimeThenClusterId(context.getHeadRevision().getRevision(context.getClusterId())) < 0);
    }

    private boolean isInSlidingWindow(Revision r) {
        return System.currentTimeMillis() - context.getSlidingWindowLength() < r.getTimestamp();
    }
}
