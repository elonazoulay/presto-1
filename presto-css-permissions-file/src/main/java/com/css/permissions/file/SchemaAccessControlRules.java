/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.css.permissions.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SchemaAccessControlRules
{
    private final String catalog;
    private final List<SchemaAccessControlRule> rules;

    @JsonCreator
    public SchemaAccessControlRules(
            @JsonProperty("catalog") String catalog,
            @JsonProperty("rules") Optional<List<SchemaAccessControlRule>> rules)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.rules = rules.map(ImmutableList::copyOf).orElse(ImmutableList.of());
    }

    public String getCatalog()
    {
        return catalog;
    }

    public List<SchemaAccessControlRule> getRules()
    {
        return rules;
    }
}
