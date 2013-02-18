//
// ESDatabase.m
// Objective-C SQLite wrapper
//
// Created by Zoran Simic on 9/13/09.
// Copyright 2009 esmiler.com. All rights reserved
//

#import "ESDatabase.h"

#define ESFS(msg...) [NSString stringWithFormat:msg]

// ---------------- Debug mode
#if DEBUG
#define ES_DEBUG 1
#endif

#if ES_DEBUG

#define ES_ASSERT(cond) if (self.crashOnError) { assert(cond); }
#define ES_LOG(msg...) if (self.logError) printf("%s\n", [ESFS(msg) UTF8String]);
#define ES_TRACE(level, msg...) if (self.traceExecution & (level)) printf("[trace] %s\n", [ESFS(msg) UTF8String]);
#define ES_CHECKF(cond, ret, msg...) if (!(cond)) { ES_LOG(msg) ES_ASSERT(cond) return (ret); }		// Check for routines for which checking may have an important performance impact

// ---------------- Release mode
#else

#define ES_ASSERT(cond)							// No assert, log or trace in release mode...
#define ES_LOG(msg...)
#define ES_TRACE(level, msg...)
#define ES_CHECKF(cond, ret, msg...)

#endif
// ----------------

#define ES_CHECK_NR(cond, msg...) if (!(cond)) { ES_LOG(msg) ES_ASSERT(cond) return; }				// Check for procedures (no return value)
#define ES_CHECK(cond, ret, msg...) if (!(cond)) { ES_LOG(msg) ES_ASSERT(cond) return (ret); }		// Check with specified return value (when condition fails)


// ------------------------
// -- class ESColumnInfo --
// ------------------------
@implementation ESColumnInfo

@synthesize cid;
@synthesize name;
@synthesize type;
@synthesize nullable;
@synthesize default_value;
@synthesize isPrimaryKey;

@end

// ----------------------
// -- class ESDatabase --
// ----------------------

@implementation ESDatabase

@synthesize dbhandle;
@synthesize name;
@synthesize pathBundle;
@synthesize pathLocal;
#if ES_DEBUG
@synthesize logError;
@synthesize crashOnError;
@synthesize traceExecution;
#endif
@synthesize busyRetries;
#if SQLITE_VERSION_NUMBER >= 3005000
@synthesize openFlags;
#endif

+ (ESDatabase *)newReadOnly:(NSString *)aName {		// New DB with 'aName' in read-only mode (this means the DB will be queried directly from the app folder, meaning it can't be modified)
	return [[ESDatabase alloc] initReadOnly:aName];
}

+ (ESDatabase *)newReadWrite:(NSString *)aName {		// New DB with 'aName' in read-write mode (this means that a copy of the DB will be made from the app folder to the iPhone's local folder)
	return [[ESDatabase alloc] initReadWrite:aName];
}

+ (ESDatabase *)newCache:(NSString *)aName {			// New DB with 'aName' in read-write mode, no initial data is available, the database is to be created once on the fly using 'schema'
	return [[ESDatabase alloc] initCache:aName];
}

+ (ESDatabase *)newMemory {							// New 'in memory' DB (not persisted)
	return [[ESDatabase alloc] initMemory];
}

+ (NSString *)sqliteLibVersion {
	return ESFS(@"%s", sqlite3_libversion());
}

// --------------
// Initialization
// --------------
- (NSString *)findBundlePath:(NSString *)aName {
	return [[[NSBundle mainBundle] resourcePath] stringByAppendingPathComponent:aName];
}

- (NSString *)findLocalPath:(NSString *)aName {
	// Search for standard documents using NSSearchPathForDirectoriesInDomains
	// First Param = Searching the documents directory
	// Second Param = Searching the Users directory and not the System
	// Expand any tildes and identify home directories.
	if ([aName characterAtIndex:0] == ':') {
		return aName;
	} else {
		NSArray *paths = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory , NSUserDomainMask, YES);
		NSString *documentsDir = [paths objectAtIndex:0];
		return [documentsDir stringByAppendingPathComponent:aName];
	}
}

- (void)initialize:(NSString *)aName bundle:(NSString *)aBundle local:(NSString *)aLocal {
	ES_CHECK_NR(pathLocal == nil && pathBundle == nil, @"'initialize' can't be called more than once")
	ES_CHECK_NR(aName != nil && aName.length > 0, @"DB name must be provided")
	ES_CHECK_NR(aBundle != nil || aLocal != nil, @"Bundle or local path must be provided")
	dbhandle = 0x00;
	name = aName;
	pathBundle = aBundle;
	pathLocal = aLocal;
	ES_CHECK_NR(aBundle == nil || pathBundle != nil, @"Can't find DB '%@' in bundle", aName)
#if ES_DEBUG
	logError = YES;			// Default state when debugging
	crashOnError = YES;
	traceExecution = 0;
#endif
	busyRetries = 3;
}

- (id)init {
	ES_CHECKF(NO, nil, @"Do not call [ESDatabase init], use one of the convenience inits")
	return nil;
}

- (id)initReadOnly:(NSString *)aName {
	if ((self = [super init])) {
		[self initialize:aName bundle:[self findBundlePath:aName] local:nil];
	}
	return self;
}

- (id)initReadWrite:(NSString *)aName {
	if ((self = [super init])) {
		[self initialize:aName bundle:[self findBundlePath:aName] local:[self findLocalPath:aName]];
	}
	return self;
}

- (id)initCache:(NSString *)aName {
	if ((self = [super init])) {
		[self initialize:aName bundle:nil local:[self findLocalPath:aName]];
	}
	return self;
}

- (id)initMemory {
	if ((self = [super init])) {
		[self initialize:@":memory:" bundle:nil local:@":memory:"];
	}
	return self;
}

- (void)dealloc {
	[self close];
	dbhandle = 0x00;
}

// ----------------
// Basic operations
// ----------------
//
// To be implemented: allow to automatically unzip a .gz DB file
//#import <zlib.h>
//#define CHUNK 16384
//
//
//ES_LOG(@"testing unzip of database");
//start = [NSDate date];
//NSString *zippedDBPath = [[[NSBundle mainBundle] resourcePath] stringByAppendingPathComponent:@"foo.db.gz"];
//NSString *unzippedDBPath = [documentsDirectory stringByAppendingPathComponent:@"foo2.db"];
//gzFile file = gzopen([zippedDBPath UTF8String], "rb");
//FILE *dest = fopen([unzippedDBPath UTF8String], "w");
//unsigned char buffer[CHUNK];
//int uncompressedLength;
//while (uncompressedLength = gzread(file, buffer, CHUNK) ) {
//	// got data out of our file
//	if(fwrite(buffer, 1, uncompressedLength, dest) != uncompressedLength || ferror(dest)) {
//		ES_LOG(@"error writing data");
//	}
//}
//fclose(dest);
//gzclose(file);
//ES_LOG(@"Finished unzipping database");

- (BOOL)automaticallyCopyDatabase {								// Automatically copy DB from .app bundle to device document folder if needed
	ES_CHECK(!dbhandle, NO, @"Can't autoCopy an already open DB")
	ES_CHECK(name != nil, NO, @"No DB name specified")
	ES_CHECK(pathBundle != nil, NO, @"No .app bundle path found, this is a cache DB")
	ES_CHECK(pathLocal != nil, NO, @"No local document path found, this is a read-only DB")
	NSError *error;
	NSFileManager *fileManager = [NSFileManager defaultManager];
	NSDictionary *localAttr = [fileManager attributesOfItemAtPath:pathLocal error:&error];
	BOOL needsCopy = NO;
	if (localAttr == nil) {
		needsCopy = YES;
	} else {
		//ESDumpDictionary(@"local db attributes", localAttr);
		NSDate *localDate;
		NSDate *appDBDate;
		if ((localDate = [localAttr objectForKey:NSFileModificationDate])) {
			ES_CHECK([fileManager fileExistsAtPath:pathBundle], NO, @"Internal error: file '%@' does not exist in .app bundle", pathBundle)
			NSDictionary *appDBAttr = [fileManager attributesOfItemAtPath:pathBundle error:&error];
			ES_CHECK(appDBAttr != nil, NO, @"Internal error: can't get attributes for '%@'", pathBundle)
			appDBDate = [appDBAttr objectForKey:NSFileModificationDate];
			//ESDumpDictionary(@"app db attributes", appDBAttr);
			ES_CHECK(appDBDate != nil, NO, @"Internal error: can't get last modification date for '%@'", pathBundle)
			needsCopy = [appDBDate compare:localDate] == NSOrderedDescending;
		} else {
			needsCopy = YES;
		}
	}
	if (needsCopy) {
		BOOL success;
		if (localAttr != nil) {
			success = [fileManager removeItemAtPath:pathLocal error:&error];
			ES_CHECK(success, NO, @"Can't delete file '%@'" , pathLocal)
		}
		success = [fileManager copyItemAtPath:pathBundle toPath:pathLocal error:&error];
		ES_CHECK(success, NO, @"Can't copy database '%@' to '%@': %@", pathBundle, pathLocal, [error localizedDescription])
		ES_TRACE(EST_LIFE, @"Copied DB '%@' to '%@'", pathBundle, pathLocal)
		return success;
	}
	return YES;
}

- (BOOL)open {
	if (dbhandle) return YES;						// Already open
	ES_CHECK(name != nil, NO, @"No DB name specified")
	if (pathBundle != nil && pathLocal != nil) [self automaticallyCopyDatabase];
	NSString *path = pathLocal != nil ? pathLocal : pathBundle;
	ES_CHECK(path != nil, NO, @"No DB path could be determined for DB '%@'", name)
#if SQLITE_VERSION_NUMBER >= 3005000
	int err = 0;
	if (openFlags != 0) {
		err = sqlite3_open_v2([path fileSystemRepresentation], &dbhandle, openFlags, NULL);
	} else {
		err = sqlite3_open([path fileSystemRepresentation], &dbhandle);
	}
#else
	int err = sqlite3_open([path fileSystemRepresentation], &dbhandle);
#endif
	ES_CHECK(err == SQLITE_OK, NO, @"Can't open DB '%@' path '%@': %d", name, path, err)
	ES_CHECK(dbhandle != nil, NO, @"Internal error: nil dbhandle for DB '%@', path '%@'", name, path)
	ES_TRACE(EST_LIFE, @"Opened DB '%@', path '%@'", name, path)
	return YES;
}

- (BOOL)close {
	if (!dbhandle) return YES;						// Already closed
	ES_TRACE(EST_LIFE, @"Closing DB %@", name)
	[beginImmediateTransactionStatement close];
	[beginExclusiveTransactionStatement close];
	[commitTransactionStatement close];
	[rollbackTransactionStatement close];
	columnInfo = nil;
	beginImmediateTransactionStatement = nil;
	beginExclusiveTransactionStatement = nil;
	commitTransactionStatement = nil;
	rollbackTransactionStatement = nil;
	int rc;
	int retries = busyRetries;
	while (retries >= 0) {
		rc = sqlite3_close(dbhandle);
		if (rc == SQLITE_BUSY) {
			if (retries-- > 0) {
				usleep(20);
			}
		} else if (rc == SQLITE_OK || rc == SQLITE_DONE) {
			dbhandle = 0x00;
			return YES;
		} else {
			return NO;
		}
	}
	return NO;
}

- (int)changes {
	if (!dbhandle) return 0;
	return sqlite3_changes(dbhandle);
}

- (BOOL)isAutoCommit {						// Is this database in 'auto-commit' mode currently (ie, no explicit transaction in progress)
	return (BOOL)sqlite3_get_autocommit(dbhandle);
}

- (BOOL)setSchema:(NSArray *)aSchema {		// Set the schema of a cache DB, this is done only once, when cache DB is first created
	ES_CHECK(pathBundle == nil, NO, @"Can set schema only for DBs iniatilized without 'pathBundle'")
	if (![self open]) {
		return NO;
	}
	if (aSchema != nil && aSchema.count > 0) {
		if (![self beginTransaction]) {
			return NO;
		}
		int i = 0;
		for (; i < aSchema.count; i++) {
			if (![self execute:[aSchema objectAtIndex:i]]) {
				return NO;
			}
		}
		return [self commit];
	}
	return YES;
}

// -----------------
// State information
// -----------------
- (BOOL)isReadOnly { return pathLocal == nil; }
- (BOOL)isCache { return pathBundle == nil; }
- (BOOL)isMemory { return [name isEqualToString:@":memory:"]; }

#if ES_DEBUG
// synthesized
#else
- (BOOL)logError { return NO; }
- (BOOL)crashOnError { return NO; }
- (int)traceExecution { return 0; }
- (void)setLogError:(BOOL)pbool { }
- (void)setCrashOnError:(BOOL)pbool { }
- (void)setTraceExecution:(int)aLevel { }
#endif

- (void)setBusyRetries:(int)aBusyRetries {
	ES_CHECK_NR(aBusyRetries >= 0, @"'busyRetries' must be >= 0, %i not allowed", aBusyRetries)
	busyRetries = aBusyRetries;
}

- (void)setOpenFlags:(int)anOpenFlags {
	ES_CHECK_NR(dbhandle == 0x00, @"Can't set 'openFlags' when DB is already open")
	openFlags = anOpenFlags;
}

- (BOOL)goodConnection {
	if (!dbhandle) return NO;
	ESResultSet *r = [self select:@"SELECT name FROM sqlite_master WHERE type='table'"];
	if (![r next]) return NO;
	return ([r stringValue:0] != nil);
}

- (BOOL)exists {
	NSError *error;
	NSFileManager *fileManager = [NSFileManager defaultManager];
	NSDictionary *localAttr = [fileManager attributesOfItemAtPath:pathLocal error:&error];
	if (localAttr != nil) {
		NSNumber *fileSize;
		if ((fileSize = [localAttr objectForKey:NSFileSize])) {
			int s = [fileSize intValue];
			return s > 100;
		}
	}
	return NO;
}

- (NSString *)lastErrorMessage {
	return [NSString stringWithUTF8String:sqlite3_errmsg(dbhandle)];
}

- (BOOL)hadError {
	int lastCode = [self lastErrorCode];
	return (lastCode > SQLITE_OK && lastCode < SQLITE_ROW);
}

- (int)lastErrorCode {
	return sqlite3_errcode(dbhandle);
}

- (sqlite_int64)lastInsertRowId {
	sqlite_int64 ret = sqlite3_last_insert_rowid(dbhandle);
	return ret;
}

// ----------
// Encryption
// ----------
- (BOOL)reKey:(NSString*)aKey {
	ES_CHECK(aKey != nil, NO, @"No key provided to 'reKey'")
#ifdef SQLITE_HAS_CODEC
	int rc = sqlite3_rekey(db, [aKey UTF8String], strlen([aKey UTF8String]));
	ES_CHECK(rc == SQLITE_OK, NO, @"Error on 'reKey': %d %@", rc, [self lastErrorMessage])
	return (rc == SQLITE_OK);
#else
	return NO;
#endif
}

- (BOOL)setKey:(NSString*)aKey {
	ES_CHECK(aKey != nil, NO, @"No key provided to 'setKey'")
#ifdef SQLITE_HAS_CODEC
	int rc = sqlite3_key(db, [aKey UTF8String], strlen([aKey UTF8String]));
	ES_CHECK(rc == SQLITE_OK, NO, @"Error on 'setKey': %d %@", rc, [self lastErrorMessage])
	return (rc == SQLITE_OK);
#else
	return NO;
#endif
}


// --------------
// SQL operations
// --------------
- (ESStatement *)prepare:(NSString *)aQuery {
	ES_CHECK(dbhandle != nil, nil, @"Can't prepare '%@': open the database first", aQuery)
	return [[ESStatement alloc] initWithQuery:aQuery database:self];
}

- (BOOL)execute:(NSString *)aQuery withArray:(NSArray *)args {
	ES_CHECK(dbhandle != nil, NO, @"Can't execute '%@': open the database first", aQuery)
	ESStatement *s = [self prepare:aQuery];
	BOOL r = [s executeWithArray:args];
	return r;
}

int esdb_placeholderCount(NSString *pstring);
int esdb_placeholderCount(NSString *pstring) {
	int pcount = 0;
	int n = pstring.length;
	int i;
	for (i = 0; i < n; i++) {
		unichar c = [pstring characterAtIndex:i];
		if (c == '?') pcount++;
	}
	return pcount;
}

- (BOOL)execute:(NSString *)aQuery, ... {
	va_list args;
	int n = esdb_placeholderCount(aQuery);
	NSMutableArray *params = nil;
	if (n > 0) {
		params = [[NSMutableArray alloc] initWithCapacity:n];
		va_start(args, aQuery);
		while (n-- > 0) {
			id obj = va_arg(args, id);
			[params addObject:obj];
		}
		va_end(args);
	}
	return [self execute:aQuery withArray:params];
}

- (ESResultSet *)select:(NSString *)aQuery withArray:(NSArray *)args {
	ES_CHECK(dbhandle != nil, nil, @"Can't select '%@': open the database first", aQuery)
	ESStatement *s = [self prepare:aQuery];
	ESResultSet *r = [s selectWithArray:args];
	return r;
}

- (ESResultSet *)select:(NSString *)aQuery, ... {
	int n = esdb_placeholderCount(aQuery);
	NSMutableArray *params = nil;
	if (n > 0) {
		params = [[NSMutableArray alloc] initWithCapacity:n];
		va_list args;
		va_start(args, aQuery);
		while (n-- > 0) {
			id obj = va_arg(args, id);
			[params addObject:obj];
		}
		va_end(args);
	}
	return [self select:aQuery withArray:params];
}

- (BOOL)beginTransaction {
	ES_CHECK(dbhandle != 0x00, NO, @"Can't begin transaction on non-open DB")
	if (beginImmediateTransactionStatement == nil) {
		beginImmediateTransactionStatement = [self prepare:@"BEGIN IMMEDIATE TRANSACTION"];
	}
	return [beginImmediateTransactionStatement execute];
}

- (BOOL)beginExclusiveTransaction {
	ES_CHECK(dbhandle != 0x00, NO, @"Can't begin exclusive transaction on non-open DB")
	if (beginExclusiveTransactionStatement == nil) {
		beginExclusiveTransactionStatement = [self prepare:@"BEGIN EXCLUSIVE TRANSACTION"];
	}
	return [beginExclusiveTransactionStatement execute];
}

- (BOOL)commit {
	ES_CHECK(dbhandle != 0x00, NO, @"Can't commit transaction on non-open DB")
	ES_CHECK(beginImmediateTransactionStatement != nil || beginExclusiveTransactionStatement != nil, NO, @"Can't commit without begin transaction")
	if (commitTransactionStatement == nil) {
		commitTransactionStatement = [self prepare:@"COMMIT TRANSACTION"];
	}
	return [commitTransactionStatement execute];
}

- (BOOL)rollback {
	ES_CHECK(dbhandle != 0x00, NO, @"Can't rollback transaction on non-open DB")
	ES_CHECK(beginImmediateTransactionStatement != nil || beginExclusiveTransactionStatement != nil, NO, @"Can't rollback without begin transaction")
	if (rollbackTransactionStatement == nil) {
		rollbackTransactionStatement = [self prepare:@"ROLLBACK TRANSACTION"];
	}
	return [rollbackTransactionStatement execute];
}

// ----------
// Reflection
// ----------
- (BOOL)tableExists:(NSString *)aTableName {
	NSString *tableName = [aTableName lowercaseString];
	ESResultSet *rs = [self select:@"select [type] from sqlite_master where [type]='table' and lower(name)=?", tableName];
	return [rs next];
}

- (NSString *)schema:(NSString *)aTableName {		// Schema for 'aTableName', or entire DB if 'aTableName' is nil
	ESResultSet *rs = [self select:@"SELECT tbl_name,[sql] FROM sqlite_master WHERE [type]='table' ORDER BY rootpage"];
	NSMutableString *sql = [NSMutableString stringWithCapacity:256];
	while ([rs next]) {
		NSString *tname = [rs stringValue:0];
		NSString *tsql = [rs stringValue:1];
		if (tname != nil && tsql != nil && (aTableName == nil || [[aTableName lowercaseString] isEqualToString:tname])) {
			if (sql.length > 0 && [sql characterAtIndex:sql.length - 1] != '\n') {
				[sql appendString:@"\n"];
			}
			[sql appendString:tsql];
		}
	}
	return sql;
}

- (NSString *)schema {
	return [self schema:nil];
}

- (ESColumnInfo *)columnInfo:(NSString *)aTableName columnName:(NSString *)aColumnName {
	if (columnInfo == nil) {
		columnInfo = [[NSMutableDictionary alloc] initWithCapacity:1];
	}
	NSString *tableName = [aTableName lowercaseString];
	NSString *columnName = [aColumnName lowercaseString];
	NSMutableDictionary *tableInfo = [columnInfo objectForKey:tableName];
	if (tableInfo == nil) {
		tableInfo = [[NSMutableDictionary alloc] initWithCapacity:4];
		ESResultSet *rs = [self select:ESFS(@"PRAGMA table_info(%@)", tableName)];
		while ([rs next]) {
			ESColumnInfo *tcol = [[ESColumnInfo alloc] init];
			tcol.cid = [rs intValue:0];
			tcol.name = [rs stringValue:1];
			tcol.type = [rs stringValue:2];
			tcol.nullable = [rs boolValue:3];
			tcol.default_value = [rs stringValue:4];
			tcol.isPrimaryKey = [rs boolValue:5];
			[tableInfo setObject:tcol forKey:tableName];
		}
		[columnInfo setObject:tableInfo forKey:tableName];
	}
	return [tableInfo objectForKey:columnName];
}

- (int)columnId:(NSString *)aTableName columnName:(NSString *)aColumnName {
	return [[self columnInfo:aTableName columnName:aColumnName] cid];
}

- (NSString *)columnType:(NSString *)aTableName columnName:(NSString *)aColumnName {
	return [[self columnInfo:aTableName columnName:aColumnName] type];
}

- (BOOL)columnNullable:(NSString *)aTableName columnName:(NSString *)aColumnName {
	return [[self columnInfo:aTableName columnName:aColumnName] nullable];
}

- (BOOL)columnExists:(NSString *)aTableName columnName:(NSString *)aColumnName {
	return [self columnInfo:aTableName columnName:aColumnName] != nil;
}

@end


// -----------------------
// -- class ESStatement --
// -----------------------
@implementation ESStatement

@synthesize sthandle;
@synthesize database;
@synthesize query;
@synthesize columnCount;
@synthesize paramCount;
@synthesize hitCount;

// --------------
// Initialization
// --------------
- (ESStatement *)initWithQuery:(NSString *)aQuery database:(ESDatabase *)aDatabase {
	if ((self = [self init])) {
		query = aQuery;
		database = aDatabase;
		columnNameToIndexMap = nil;
	}
	return self;
}

- (void)dealloc {
	[self close];
}

- (BOOL)close {
	if (!sthandle) return YES;
	int rc = sqlite3_finalize(sthandle);
	if (rc == SQLITE_OK) {
		sthandle = 0x00;
		ES_TRACE(EST_LIFE, @"Closed statement '%@'", query)
		return YES;
	} else {
		ES_TRACE(EST_LIFE, @"Failed to close statement (error code %i) '%@'", rc, query)
		return NO;
	}
}

- (void)reset {						// Reset an executed prepared statement
	if (!sthandle) return;
	sqlite3_reset(sthandle);
	sqlite3_clear_bindings(sthandle);
}


- (BOOL)prepare {
	if (sthandle) return YES;
	ES_TRACE(EST_LIFE, @"prepare: %@", query)
	int rc = SQLITE_ERROR;
	int retries = self.busyRetries;
	while (retries >= 0) {
		rc = sqlite3_prepare(database.dbhandle, [query UTF8String], -1, &sthandle, 0);
		if (rc == SQLITE_BUSY && retries-- > 0) {
			if (retries <= 0) {
				[self close];
				ES_CHECKF(NO, NO, @"Can't prepare '%@' after %i retries", query, self.busyRetries)
				return NO;
			}
			usleep(20);
		} else {
			retries = -1;
		}
	}
	ES_CHECK(rc == SQLITE_OK, NO, @"Can't prepare '%@': %i", query, rc)
	paramCount = sqlite3_bind_parameter_count(sthandle);
	columnCount = sqlite3_column_count(sthandle);
	return YES;
}

// -----------------
// State information
// -----------------
- (int)busyRetries { return database.busyRetries; }
- (BOOL)logError { return database.logError; }
- (BOOL)crashOnError { return database.crashOnError; }
- (int)traceExecution { return database.traceExecution; }

- (NSString *)description {
	return ESFS(@"%@ %ld hit(s) for query %@", [super description], hitCount, query);
}

- (int)columnIndex:(NSString *)aColumnName {
	if (columnNameToIndexMap == nil) {
		columnNameToIndexMap = [[NSMutableDictionary alloc] init];
		int i = columnCount - 1;
		for (; i >= 0; i--) {
			NSString *key = [[NSString stringWithUTF8String:sqlite3_column_name(sthandle, i)] lowercaseString];
			[columnNameToIndexMap setObject:ES_INT(i) forKey:key];
		}
	}
	NSNumber *n = [columnNameToIndexMap objectForKey:[aColumnName lowercaseString]];
	ES_CHECK(n != nil, -1, @"No column name %@ in '%@'", aColumnName, query)
	return [n intValue];
}

// --------------
// SQL operations
// --------------
- (BOOL)bindObject:(id)obj toColumn:(int)idx {
	int rc;
	ES_TRACE(EST_QUERY, @" param #%i: %@", idx, obj)
	if ((!obj) || ((NSNull *)obj == [NSNull null])) {
		rc = sqlite3_bind_null(sthandle, idx);
	} else if ([obj isKindOfClass:[NSNumber class]]) {
		if (strcmp([obj objCType], @encode(BOOL)) == 0) {
			rc = sqlite3_bind_int(sthandle, idx, ([obj boolValue] ? 1 : 0));
		} else if (strcmp([obj objCType], @encode(int)) == 0) {
			rc = sqlite3_bind_int64(sthandle, idx, [obj longValue]);
		} else if (strcmp([obj objCType], @encode(long)) == 0) {
			rc = sqlite3_bind_int64(sthandle, idx, [obj longValue]);
		} else if (strcmp([obj objCType], @encode(float)) == 0 || strcmp([obj objCType], @encode(double)) == 0) {
			rc = sqlite3_bind_double(sthandle, idx, [obj doubleValue]);
		} else {
			rc = sqlite3_bind_text(sthandle, idx, [[obj description] UTF8String], -1, SQLITE_STATIC);
		}
	} else if ([obj isKindOfClass:[NSDate class]]) {
		rc = sqlite3_bind_double(sthandle, idx, [obj timeIntervalSince1970]);
	} else if ([obj isKindOfClass:[NSData class]]) {
		rc = sqlite3_bind_blob(sthandle, idx, [obj bytes], [obj length], SQLITE_STATIC);
	} else {
		rc = sqlite3_bind_text(sthandle, idx, [[obj description] UTF8String], -1, SQLITE_STATIC);
	}
	ES_CHECK(rc == SQLITE_OK, NO, @"Can't bind param #%i to '%@': %i", idx, obj, rc)
	return YES;
}

- (BOOL)executeWithArray:(NSArray *)args {		// Execute with arguments passed as an NSArray
	if (![self prepare]) return NO;
	ES_CHECK(args.count == paramCount, NO, @"Wrong number of arguments to SQL query '%@': %i instead of %i", query, args.count, paramCount)
	ES_TRACE(EST_QUERY, @"execute: %@", query)
	int idx = 0;
	BOOL stop = NO;
	id obj;
	while (!stop && idx < paramCount) {
		obj =[args objectAtIndex:idx];
		idx++;
		if (![self bindObject:obj toColumn:idx]) stop = YES;
	}
	ES_CHECK(!stop, NO, @"Could not bind param %i for query '%@'", idx, query)
	ES_CHECK(idx == paramCount, NO, @"Got %i params while expecting %i for query '%@'", idx, paramCount, query)
	int rc = SQLITE_ERROR;
	int retries = self.busyRetries;
	while (retries >= 0) {
		rc = sqlite3_step(sthandle);
		if ((rc == SQLITE_BUSY) && (retries-- > 0)) {
			if (retries <= 0) {
				ES_CHECK(NO, NO, @"Can't step '%@' after %i retries", query, self.busyRetries)
			}
			usleep(20);
		} else {
			retries = -1;
		}
	}
	ES_CHECK(rc == SQLITE_DONE || rc == SQLITE_ROW, NO, @"Error calling sqlite3_step (code %d: %@)", rc, database.lastErrorMessage)
	hitCount++;
	return (rc == SQLITE_DONE || rc == SQLITE_ROW);
}

- (BOOL)execute:(id)arg1, ... {					// Execute with a variable number of arguments
	va_list args;
	int n = esdb_placeholderCount(query);
	NSMutableArray *params = nil;
	if (n > 0) {
		ES_CHECK(arg1 != nil, NO, @"First agument to 'execute:' shouldn't be nil")
		params = [[NSMutableArray alloc] initWithCapacity:n];
		[params addObject:arg1];
		va_start(args, arg1);
		while (n-- > 1) {
			id obj = va_arg(args, id);
			[params addObject:obj];
		}
		va_end(args);
	}
	BOOL result = [self executeWithArray:params];
	[self reset];
	return result;
}

- (BOOL)execute {								// Execute without arguments
	return [self executeWithArray:nil];
}

- (ESResultSet *)selectWithArray:(NSArray *)args {
	if (![self prepare]) return nil;
	ES_TRACE(EST_QUERY, @"select: %@", query)
	ESResultSet *rs = [[ESResultSet alloc] initWithStatement:self];
	int idx = 0;
	id obj;
	while (idx < paramCount) {
		obj = [args objectAtIndex:idx];
		idx++;
		if (![self bindObject:obj toColumn:idx]) return nil;
	}
	ES_CHECK(idx == paramCount, nil, @"Got %i params while expecting %i in %@", idx, paramCount, query)
	hitCount++;
	return rs;
}

- (ESResultSet *)select:(id)arg1, ... {
	va_list args;
	int n = esdb_placeholderCount(query);
	NSMutableArray *params = nil;
	if (n > 0) {
		ES_CHECK(arg1 != nil, nil, @"First agument to 'select:' shouldn't be nil")
		params = [[NSMutableArray alloc] initWithCapacity:n];
		[params addObject:arg1];
		va_start(args, arg1);
		while (n-- > 1) {
			id obj = va_arg(args, id);
			[params addObject:obj];
		}
		va_end(args);
	}
	return [self selectWithArray:params];
}

- (ESResultSet *)select {
	return [self selectWithArray:nil];
}

@end


// -----------------------
// -- class ESResultSet --
// -----------------------
@implementation ESResultSet

@synthesize statement;

// --------------
// Initialization
// --------------
- (ESResultSet *)initWithStatement:(ESStatement *)aStatement {
	if ((self = [self init])) {
		statement = aStatement;
	}
	return self;
}

// -----------------
// State information
// -----------------
- (int)busyRetries { return statement.busyRetries; }
- (ESDatabase *)database { return [statement database]; }
- (NSString *)query { return [statement query]; }
- (BOOL)logError { return [statement logError]; }
- (BOOL)crashOnError { return [statement crashOnError]; }
- (int)traceExecution { return [statement traceExecution]; }

// --------------
// SQL operations
// --------------
- (void)close {				// Close result set, object can't be used anymore after this
	[statement reset];
}

- (BOOL)next {
	if (statement == nil) return NO;
	int rc = SQLITE_ERROR;
	int retries = self.busyRetries;
	while (retries >= 0) {
		rc = sqlite3_step(statement.sthandle);
		if (rc == SQLITE_BUSY && retries-- > 0) {
			if (retries <= 0) {
				[self close];
				ES_CHECK(NO, NO, @"Can't step '%@' after %i retries", statement.query, self.busyRetries)
			}
			usleep(20);
		} else {
			retries = -1;
		}
	}
	ES_CHECK(rc == SQLITE_DONE || rc == SQLITE_ROW, NO, @"Error calling sqlite3_step (code %d: %@)", rc, statement.database.lastErrorMessage)
	if (rc != SQLITE_ROW) [self close];
	return (rc == SQLITE_ROW);
}

// -----------------------
// Data retrieval by index
// -----------------------
- (int)intValue:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, 0, @"Invalid int column %i requested for %@", aColumnIndex, statement.query)
	return sqlite3_column_int(statement.sthandle, aColumnIndex);
}

- (long)longValue:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, 0, @"Invalid long column %i requested for %@", aColumnIndex, statement.query)
	return (long)sqlite3_column_int64(statement.sthandle, aColumnIndex);
}

- (long long int)longLongIntValue:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, 0, @"Invalid long long int column %i requested for %@", aColumnIndex, statement.query)
	return sqlite3_column_int64(statement.sthandle, aColumnIndex);
}

- (BOOL)boolValue:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, NO, @"Invalid bool column %i requested for %@", aColumnIndex, statement.query)
	return [self intValue:aColumnIndex] != 0;
}

- (float)floatValue:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, 0, @"Invalid float column %i requested for %@", aColumnIndex, statement.query)
	return (float)sqlite3_column_double(statement.sthandle, aColumnIndex);
}

- (double)doubleValue:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, 0, @"Invalid double column %i requested for %@", aColumnIndex, statement.query)
	return sqlite3_column_double(statement.sthandle, aColumnIndex);
}

- (NSString *)stringValue:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, nil, @"Invalid string column %i requested for %@", aColumnIndex, statement.query)
	const char *c = (const char *)sqlite3_column_text(statement.sthandle, aColumnIndex);
	if (!c) return nil;		// null row.
	NSString *s = [NSString stringWithUTF8String:c];
#if ES_DEBUG
	if (s == nil) { ES_LOG(@"--> [%s] -> []", c) }
#endif
	return s;
}

- (NSDate *)dateValueWithTimeIntervalSince1970:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, nil, @"Invalid date column %i requested for %@", aColumnIndex, statement.query)
	return [NSDate dateWithTimeIntervalSince1970:[self doubleValue:aColumnIndex]];
}

- (NSDate *)dateValueCCYYMMDD:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, nil, @"Invalid date column %i requested for %@", aColumnIndex, statement.query)
	static NSDateFormatter *dateFormatter = nil;
	if (dateFormatter == nil) {
		dateFormatter = [[NSDateFormatter alloc] init];
		[dateFormatter setDateFormat:@"yyyyMMdd"];
	}
	NSString *s = [self stringValue:aColumnIndex];
	if (s == nil) return nil;
	return [dateFormatter dateFromString:s];
}

- (NSDate *)dateValueCCYYMMDDdashed:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, nil, @"Invalid date column %i requested for %@", aColumnIndex, statement.query)
	static NSDateFormatter *dateFormatter = nil;
	if (dateFormatter == nil) {
		dateFormatter = [[NSDateFormatter alloc] init];
		[dateFormatter setDateFormat:@"yyyy-MM-dd"];
	}
	NSString *s = [self stringValue:aColumnIndex];
	if (s == nil) return nil;
	return [dateFormatter dateFromString:s];
}

- (NSDate *)dateValueCCYYMMDDslashed:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, nil, @"Invalid date column %i requested for %@", aColumnIndex, statement.query)
	static NSDateFormatter *dateFormatter = nil;
	if (dateFormatter == nil) {
		dateFormatter = [[NSDateFormatter alloc] init];
		[dateFormatter setDateFormat:@"yyyy/MM/dd"];
	}
	NSString *s = [self stringValue:aColumnIndex];
	if (s == nil) return nil;
	return [dateFormatter dateFromString:s];
}

- (NSData *)dataValue:(int)aColumnIndex {
	ES_CHECKF(aColumnIndex >= 0 && aColumnIndex < statement.columnCount, nil, @"Invalid data column %i requested for %@", aColumnIndex, statement.query)
	int dataSize = sqlite3_column_bytes(statement.sthandle, aColumnIndex);
	if (dataSize <= 0) return nil;
	NSMutableData *data = [NSMutableData dataWithLength:dataSize];
	memcpy([data mutableBytes], sqlite3_column_blob(statement.sthandle, aColumnIndex), dataSize);
	return data;
}

// -----------------------------
// Data retrieval by column name
// -----------------------------
- (int)intValueByName:(NSString *)aColumnName { return [self intValue:[statement columnIndex:aColumnName]]; }
- (long)longValueByName:(NSString *)aColumnName { return [self longValue:[statement columnIndex:aColumnName]]; }
- (long long int)longLongIntValueByName:(NSString *)aColumnName { return [self longLongIntValue:[statement columnIndex:aColumnName]]; }
- (BOOL)boolValueByName:(NSString *)aColumnName { return [self boolValue:[statement columnIndex:aColumnName]]; }
- (float)floatValueByName:(NSString *)aColumnName { return [self floatValue:[statement columnIndex:aColumnName]]; }
- (double)doubleValueByName:(NSString *)aColumnName { return [self doubleValue:[statement columnIndex:aColumnName]]; }
- (NSString *)stringValueByName:(NSString *)aColumnName { return [self stringValue:[statement columnIndex:aColumnName]]; }
- (NSDate *)dateValueCCYYMMDDByName:(NSString *)aColumnName { return [self dateValueCCYYMMDD:[statement columnIndex:aColumnName]]; }
- (NSData *)dataValueByName:(NSString *)aColumnName { return [self dataValue:[statement columnIndex:aColumnName]]; }

@end


