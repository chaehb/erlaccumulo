-ifndef(_proxy_types_included).
-define(_proxy_types_included, yeah).

-define(proxy_PartialKey_ROW, 0).
-define(proxy_PartialKey_ROW_COLFAM, 1).
-define(proxy_PartialKey_ROW_COLFAM_COLQUAL, 2).
-define(proxy_PartialKey_ROW_COLFAM_COLQUAL_COLVIS, 3).
-define(proxy_PartialKey_ROW_COLFAM_COLQUAL_COLVIS_TIME, 4).
-define(proxy_PartialKey_ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL, 5).

-define(proxy_TablePermission_READ, 2).
-define(proxy_TablePermission_WRITE, 3).
-define(proxy_TablePermission_BULK_IMPORT, 4).
-define(proxy_TablePermission_ALTER_TABLE, 5).
-define(proxy_TablePermission_GRANT, 6).
-define(proxy_TablePermission_DROP_TABLE, 7).

-define(proxy_SystemPermission_GRANT, 0).
-define(proxy_SystemPermission_CREATE_TABLE, 1).
-define(proxy_SystemPermission_DROP_TABLE, 2).
-define(proxy_SystemPermission_ALTER_TABLE, 3).
-define(proxy_SystemPermission_CREATE_USER, 4).
-define(proxy_SystemPermission_DROP_USER, 5).
-define(proxy_SystemPermission_ALTER_USER, 6).
-define(proxy_SystemPermission_SYSTEM, 7).

-define(proxy_ScanType_SINGLE, 0).
-define(proxy_ScanType_BATCH, 1).

-define(proxy_ScanState_IDLE, 0).
-define(proxy_ScanState_RUNNING, 1).
-define(proxy_ScanState_QUEUED, 2).

-define(proxy_CompactionType_MINOR, 0).
-define(proxy_CompactionType_MERGE, 1).
-define(proxy_CompactionType_MAJOR, 2).
-define(proxy_CompactionType_FULL, 3).

-define(proxy_CompactionReason_USER, 0).
-define(proxy_CompactionReason_SYSTEM, 1).
-define(proxy_CompactionReason_CHOP, 2).
-define(proxy_CompactionReason_IDLE, 3).
-define(proxy_CompactionReason_CLOSE, 4).

-define(proxy_IteratorScope_MINC, 0).
-define(proxy_IteratorScope_MAJC, 1).
-define(proxy_IteratorScope_SCAN, 2).

-define(proxy_TimeType_LOGICAL, 0).
-define(proxy_TimeType_MILLIS, 1).

%% struct key

-record(key, {row :: string() | binary(),
              colFamily :: string() | binary(),
              colQualifier :: string() | binary(),
              colVisibility :: string() | binary(),
              timestamp :: integer()}).

%% struct columnUpdate

-record(columnUpdate, {colFamily :: string() | binary(),
                       colQualifier :: string() | binary(),
                       colVisibility :: string() | binary(),
                       timestamp :: integer(),
                       value :: string() | binary(),
                       deleteCell :: boolean()}).

%% struct keyValue

-record(keyValue, {key :: #key{},
                   value :: string() | binary()}).

%% struct scanResult

-record(scanResult, {results :: list(),
                     more :: boolean()}).

%% struct range

-record(range, {start :: #key{},
                startInclusive :: boolean(),
                stop :: #key{},
                stopInclusive :: boolean()}).

%% struct scanColumn

-record(scanColumn, {colFamily :: string() | binary(),
                     colQualifier :: string() | binary()}).

%% struct iteratorSetting

-record(iteratorSetting, {priority :: integer(),
                          name :: string() | binary(),
                          iteratorClass :: string() | binary(),
                          properties :: dict()}).

%% struct scanOptions

-record(scanOptions, {authorizations :: set(),
                      range :: #range{},
                      columns :: list(),
                      iterators :: list(),
                      bufferSize :: integer()}).

%% struct batchScanOptions

-record(batchScanOptions, {authorizations :: set(),
                           ranges :: list(),
                           columns :: list(),
                           iterators :: list(),
                           threads :: integer()}).

%% struct keyValueAndPeek

-record(keyValueAndPeek, {keyValue :: #keyValue{},
                          hasNext :: boolean()}).

%% struct keyExtent

-record(keyExtent, {tableId :: string() | binary(),
                    endRow :: string() | binary(),
                    prevEndRow :: string() | binary()}).

%% struct column

-record(column, {colFamily :: string() | binary(),
                 colQualifier :: string() | binary(),
                 colVisibility :: string() | binary()}).

%% struct activeScan

-record(activeScan, {client :: string() | binary(),
                     user :: string() | binary(),
                     table :: string() | binary(),
                     age :: integer(),
                     idleTime :: integer(),
                     type :: integer(),
                     state :: integer(),
                     extent :: #keyExtent{},
                     columns :: list(),
                     iterators :: list(),
                     authorizations :: list()}).

%% struct activeCompaction

-record(activeCompaction, {extent :: #keyExtent{},
                           age :: integer(),
                           inputFiles :: list(),
                           outputFile :: string() | binary(),
                           type :: integer(),
                           reason :: integer(),
                           localityGroup :: string() | binary(),
                           entriesRead :: integer(),
                           entriesWritten :: integer(),
                           iterators :: list()}).

%% struct writerOptions

-record(writerOptions, {maxMemory :: integer(),
                        latencyMs :: integer(),
                        timeoutMs :: integer(),
                        threads :: integer()}).

%% struct unknownScanner

-record(unknownScanner, {msg :: string() | binary()}).

%% struct unknownWriter

-record(unknownWriter, {msg :: string() | binary()}).

%% struct noMoreEntriesException

-record(noMoreEntriesException, {msg :: string() | binary()}).

%% struct accumuloException

-record(accumuloException, {msg :: string() | binary()}).

%% struct accumuloSecurityException

-record(accumuloSecurityException, {msg :: string() | binary()}).

%% struct tableNotFoundException

-record(tableNotFoundException, {msg :: string() | binary()}).

%% struct tableExistsException

-record(tableExistsException, {msg :: string() | binary()}).

%% struct mutationsRejectedException

-record(mutationsRejectedException, {msg :: string() | binary()}).

-endif.
