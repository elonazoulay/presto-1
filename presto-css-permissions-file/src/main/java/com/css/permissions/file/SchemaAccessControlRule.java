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

import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class SchemaAccessControlRule
{
    private final AccessMode accessMode;
    private final Optional<Pattern> groupRegex;
    private final Optional<Pattern> schemaRegex;

    @JsonCreator
    public SchemaAccessControlRule(
            @JsonProperty("allow") AccessMode accessMode,
            @JsonProperty("groupRegex") Optional<Pattern> groupRegex,
            @JsonProperty("schemaRegex") Optional<Pattern> schemaRegex)
    {
        this.accessMode = requireNonNull(accessMode, "accessMode is null");
        this.groupRegex = requireNonNull(groupRegex, "groupRegex is null");
        this.schemaRegex = requireNonNull(schemaRegex, "schemaRegex is null");
    }

    public Optional<AccessMode> match(String group, String schema)
    {
        if (groupRegex.map(regex -> regex.matcher(group).matches()).orElse(true) &&
                this.schemaRegex.map(regex -> regex.matcher(schema).matches()).orElse(true)) {
            return Optional.of(accessMode);
        }
        return Optional.empty();
    }
}
