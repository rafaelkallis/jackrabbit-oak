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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * Predicate on property values. This property holds if and only if
 * the tree has a property of the given {@code name} and the given
 * {@code propertyPredicate} holds on that property.
 */
public class PropertyPredicate implements Predicate<Tree> {
    private final String name;
    private final Predicate<PropertyState> propertyPredicate;

    /**
     * @param name               name of the property
     * @param propertyPredicate  predicate on the named property
     */
    public PropertyPredicate(String name, Predicate<PropertyState> propertyPredicate) {
        this.name = name;
        this.propertyPredicate = propertyPredicate;
    }

    @Override
    public boolean apply(Tree tree) {
        PropertyState property = tree.getProperty(name);
        return property != null && propertyPredicate.apply(property);
    }
}
