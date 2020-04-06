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
package io.prestosql.plugin.google.sheets;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.cache.CacheLoader.from;
import static io.prestosql.plugin.google.sheets.SheetRange.parseRange;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_BAD_CREDENTIALS_ERROR;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_METASTORE_ERROR;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_TABLE_LOAD_ERROR;
import static io.prestosql.plugin.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Locale.filter;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SheetsClient
{
    private static final Logger log = Logger.get(SheetsClient.class);

    private static final String APPLICATION_NAME = "presto google sheets integration";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    @VisibleForTesting
    static final Pattern SHEET_REGEX = Pattern.compile("(?<sheetId>.*?)(?:#(?:(?<tab>[^!].*?))?(?:!(?<range>(?<begin>\\$\\d+):\\$\\d+))?)?$");

    @VisibleForTesting
    static final String HEADER_RANGE = "$1:$1";

    private static final List<String> SCOPES = ImmutableList.of(SheetsScopes.SPREADSHEETS_READONLY);

    private final LoadingCache<String, Optional<String>> tableSheetMappingCache;
    private final LoadingCache<String, List<List<Object>>> metadataSheetCache;
    private final LoadingCache<String, List<List<Object>>> sheetDataCache;
    private final LoadingCache<String, List<Object>> columnMetadataCache;

    private final String metadataSheetId;
    private final String credentialsFilePath;

    private final Sheets sheetsService;

    @Inject
    public SheetsClient(SheetsConfig config, JsonCodec<Map<String, List<SheetsTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        this.metadataSheetId = config.getMetadataSheetId().trim();
        this.credentialsFilePath = config.getCredentialsFilePath();

        try {
            this.sheetsService = new Sheets.Builder(newTrustedTransport(), JSON_FACTORY,
                    setTimeout(getCredentials(),
                            (int) config.getSheetsReadTimeout().toMillis(),
                            (int) config.getSheetsConnectTimeout().toMillis()))
                    .setApplicationName(APPLICATION_NAME).build();
        }
        catch (GeneralSecurityException | IOException e) {
            throw new PrestoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
        long expiresAfterWriteMillis = config.getSheetsDataExpireAfterWrite().toMillis();
        long maxCacheSize = config.getSheetsDataMaxCacheSize();

        this.tableSheetMappingCache = newCacheBuilder(expiresAfterWriteMillis, maxCacheSize)
                .build(new CacheLoader<String, Optional<String>>() {
                    @Override
                    public Optional<String> load(String tableName)
                    {
                        return getSheetExpressionForTable(tableName);
                    }

                    @Override
                    public Map<String, Optional<String>> loadAll(Iterable<? extends String> tableList)
                    {
                        return getAllTableSheetExpressionMapping();
                    }
                });
        this.metadataSheetCache = newCacheBuilder(expiresAfterWriteMillis, maxCacheSize).build(from(this::readAllValuesFromSheetExpression));
        this.columnMetadataCache = newCacheBuilder(expiresAfterWriteMillis, maxCacheSize).build(from(this::readHeaderFromSheetExpression));
        this.sheetDataCache = newCacheBuilder(expiresAfterWriteMillis, maxCacheSize).build(from(this::readAllValuesFromSheetExpression));
    }

    public Optional<SheetsTable> getTable(String tableName)
    {
        List<Object> header = readHeader(tableName);
        if (!header.isEmpty()) {
            ImmutableList.Builder<SheetsColumn> columns = ImmutableList.builder();
            Set<String> columnNames = new HashSet<>();
            // Assuming 1st line is always header
            int count = 0;
            for (Object column : header) {
                String columnValue = column.toString().toLowerCase(ENGLISH);
                // when empty or repeated column header, adding a placeholder column name
                if (columnValue.isEmpty() || columnNames.contains(columnValue)) {
                    columnValue = "column_" + ++count;
                }
                columnNames.add(columnValue);
                columns.add(new SheetsColumn(columnValue, VarcharType.VARCHAR));
            }
            return Optional.of(new SheetsTable(tableName, columns.build()));
        }
        return Optional.empty();
    }

    public List<List<Object>> getData(SheetsTableHandle tableHandle, SheetsSplit split)
    {
        List<List<Object>> values = readAllValues(tableHandle.getTableName());
        sheetsService.spreadsheets().values().get(split.getSheetDataLocation().getSheetId(), split.getSheetDataLocation().getTabAndRange());
        if (values == null || values.isEmpty()) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }
        return values.subList(1, values.size());
    }

    public Set<String> getTableNames()
    {
        ImmutableSet.Builder<String> tables = ImmutableSet.builder();
        try {
            List<List<Object>> tableMetadata = metadataSheetCache.getUnchecked(metadataSheetId);
            for (int i = 1; i < tableMetadata.size(); i++) {
                if (tableMetadata.get(i).size() > 0) {
                    tables.add(String.valueOf(tableMetadata.get(i).get(0)));
                }
            }
            return tables.build();
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw new PrestoException(SHEETS_METASTORE_ERROR, e);
        }
    }

    private List<List<Object>> readAllValues(String tableName)
    {
        try {
            Optional<String> sheetExpression = tableSheetMappingCache.getUnchecked(tableName);
            if (!sheetExpression.isPresent()) {
                throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, "Sheet expression not found for table " + tableName);
            }
            return sheetDataCache.getUnchecked(sheetExpression.get());
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw new PrestoException(SHEETS_TABLE_LOAD_ERROR, "Error loading data for table: " + tableName, e);
        }
    }

    private List<Object> readHeader(String tableName)
    {
        try {
            Optional<String> sheetExpression = tableSheetMappingCache.getUnchecked(tableName);
            if (!sheetExpression.isPresent()) {
                throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, "Sheet expression not found for table " + tableName);
            }
            return columnMetadataCache.getUnchecked(sheetExpression.get());
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw new PrestoException(SHEETS_TABLE_LOAD_ERROR, "Error loading data for table: " + tableName, e);
        }
    }

    private Optional<String> getSheetExpressionForTable(String tableName)
    {
        Map<String, Optional<String>> tableSheetMap = getAllTableSheetExpressionMapping();
        if (!tableSheetMap.containsKey(tableName)) {
            return Optional.empty();
        }
        return tableSheetMap.get(tableName);
    }

    private Map<String, Optional<String>> getAllTableSheetExpressionMapping()
    {
        ImmutableMap.Builder<String, Optional<String>> tableSheetMap = ImmutableMap.builder();
        List<List<Object>> data = readAllValuesFromSheetExpression(metadataSheetId);
        // first line is assumed to be sheet header
        for (int i = 1; i < data.size(); i++) {
            if (data.get(i).size() >= 2) {
                String tableId = String.valueOf(data.get(i).get(0));
                String sheetId = String.valueOf(data.get(i).get(1));
                tableSheetMap.put(tableId.toLowerCase(Locale.ENGLISH), Optional.of(sheetId));
            }
        }
        return tableSheetMap.build();
    }

    private Credential getCredentials()
    {
        try (InputStream in = new FileInputStream(credentialsFilePath)) {
            return GoogleCredential.fromStream(in).createScoped(SCOPES);
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
    }

    private HttpRequestInitializer setTimeout(HttpRequestInitializer initializer, int readTimeout, int connectTimeout)
    {
        return request -> {
            initializer.initialize(request);
            request.setReadTimeout(readTimeout);
            request.setConnectTimeout(connectTimeout);
        };
    }

    private List<List<Object>> readAllValuesFromSheetExpression(String sheetExpression)
    {
        try {
            // by default loading up to 10k rows from the first tab of the sheet
            String defaultRange = "$1:$10000";
            String[] tableOptions = sheetExpression.split("#");
            String sheetId = tableOptions[0];
            if (tableOptions.length > 1) {
                defaultRange = tableOptions[1];
            }
            log.debug("Accessing sheet id [%s] with range [%s]", sheetId, defaultRange);
            return sheetsService.spreadsheets().values().get(sheetId, defaultRange).execute().getValues();
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, "Failed reading data from sheet: " + sheetExpression, e);
        }
    }

    private List<Object> readHeaderFromSheetExpression(String sheetExpression)
    {
        try {
            Matcher matcher = SHEET_REGEX.matcher(sheetExpression);
            if (!matcher.matches()) {
                return ImmutableList.of();
            }
            String sheetId = requireNonNull(matcher.group("sheetId"), "sheetId is null");
            Optional<String> tab = Optional.ofNullable(matcher.group("tab"));
            Optional<String> begin = Optional.ofNullable(matcher.group("begin"));
            List<List<Object>> values = sheetsService.spreadsheets().values().get(sheetId, getHeaderRange(tab, begin)).execute().getValues();
            if (values.isEmpty()) {
                return ImmutableList.of();
            }
            return values.get(0);
        }
        catch (Exception e) {
            throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, "Failed reading header from sheet: " + sheetExpression, e);
        }
    }

    public SheetDataLocation parseDataLocation(String tableName)
    {
        return parseDataLocation(tableName, false);
    }

    public SheetDataLocation parseDataLocationNoHeader(String tableName)
    {
        return parseDataLocation(tableName, true);
    }

    private SheetDataLocation parseDataLocation(String tableName, boolean noHeader)
    {
        try {
            requireNonNull(tableName, "sheetExpression is null");

            Optional<String> sheetExpression = tableSheetMappingCache.getUnchecked(tableName);
            if (sheetExpression == null || !sheetExpression.isPresent()) {
                throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, "Sheet expression not found for table " + tableName);
            }

            Matcher matcher = SHEET_REGEX.matcher(sheetExpression.get());
            checkState(matcher.matches(), "Malformed sheet expression: %s", sheetExpression);
            String sheetId = requireNonNull(matcher.group("sheetId"), "sheetId is null");
            Optional<String> tab = Optional.ofNullable(matcher.group("tab"));
            Optional<String> rangeExpression = Optional.ofNullable(matcher.group("range"));

            if (tab.isPresent() && rangeExpression.isPresent()) {
                return new SheetDataLocation(sheetId, tab.get(), parseRange(rangeExpression.get()));
            }

            List<Sheet> sheets = sheetsService.spreadsheets().get(sheetId).execute().getSheets();
            checkState(sheets != null && !sheets.isEmpty(), "sheets is null or empty");

            if (!tab.isPresent()) {
                tab = Optional.of(sheets.get(0).getProperties().getTitle());
            }
            SheetRange range;
            if (rangeExpression.isPresent()) {
                range = parseRange(rangeExpression.get());
            }
            else {
                String title = tab.get();
                Optional<Sheet> filteredSheet = sheets.stream()
                        .filter(sheet -> sheet.getProperties().getTitle().equals(title))
                        .findAny();
                checkState(filteredSheet.isPresent(), "Sheet not found");
                range = new SheetRange(1, filteredSheet.get().getProperties().getGridProperties().getRowCount());
            }
            if (noHeader) {
                range = new SheetRange(range.getBegin() + 1, range.getEnd());
            }
            return new SheetDataLocation(sheetId, tab.get(), range);
        }
        catch (Exception e) {
            throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, format("Failed to parse '%s':", tableName), e);
        }
    }

    @VisibleForTesting
    static String getHeaderRange(Optional<String> tab, Optional<String> begin)
    {
        String range = begin.map(first -> format("%1$s:%1$s", first)).orElse(HEADER_RANGE);
        return tab.map(name -> format("%s!%s", name, range)).orElse(range);
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteMillis, long maximumSize)
    {
        return CacheBuilder.newBuilder().expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS).maximumSize(maximumSize);
    }
}
