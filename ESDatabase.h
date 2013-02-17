//
// ESDatabase.h
// Objective-C SQLite wrapper
//
// Created by Zoran Simic on 9/13/09.
// Copyright 2009 esmiler.com. All rights reserved

/*

Add libsqlite3.dylib to your project's Frameworks

Synopsis:
	ESDatabase *db = [ESDatabase newReadWrite:@"mydb"];
	[db open];
	ESResultSet *r = [db execute:@"SELECT name,age,birthday FROM person"];
	ESStatement *sthr = [db prepare:@"SELECT name,age,birthday FROM person"];
	ESResultSet *r = [sthr execute];
	while ([r next]) { ES_LOG(@"name: %@, age: %i", [r stringValue:0], [r intValue:1]); }

	[db execute:@"UPDATE age=?,birthday=? FROM person WHERE name=?", ES_INT(21), date, @"Joe"];
	ESStatement *sthu = [db prepare:@"UPDATE age=?,birthday=? FROM person WHERE name=?"];
	[sthu execute:ES_INT(21), date, @"Joe"];
	ESResultSet *r = [sthu execute:ES_INT(21), date, @"Joe"]

 */

#import <Foundation/Foundation.h>
#import "sqlite3.h"

// Convenience feature allowing to wrap NSNumber-s in a more concise way
#define ES_INT(aInteger)		[NSNumber numberWithInt:(aInteger)]
#define ES_FLOAT(aFloat)		[NSNumber numberWithFloat:(aFloat)]
#define ES_DOUBLE(aDouble)		[NSNumber numberWithDouble:(aDouble)]

// Trace levels
#define EST_RELEASE 1			// Trace objects being released (works only when ARC is off)
#define EST_LIFE 2				// Trace statement preparing/opening/closing
#define EST_QUERY 4				// Trace queries
#define EST_FILE_OPERATIONS 8	// Trace file operations

@class ESStatement;
@class ESResultSet;

// ------------------------
// -- class ESColumnInfo --
// ------------------------
@interface ESColumnInfo : NSObject {
	int cid;
	NSString *name;
	NSString *type;
	BOOL nullable;
	NSString *default_value;
	BOOL isPrimaryKey;
}

@property (nonatomic, assign) int cid;
@property (nonatomic, retain) NSString *name;
@property (nonatomic, retain) NSString *type;
@property (nonatomic, assign) BOOL nullable;
@property (nonatomic, retain) NSString *default_value;
@property (nonatomic, assign) BOOL isPrimaryKey;

@end

// ----------------------
// -- class ESDatabase --
// ----------------------
@interface ESDatabase : NSObject {
	sqlite3 *dbhandle;				// Underlying database handle
	NSString *name;					// Database name (this is the basename part, without the extension)
	NSString *pathBundle;			// Path to SQLite DB in the .app folder
	NSString *pathLocal;			// Path to SQLite DB in the documents folder on the device
#if ES_DEBUG
	BOOL logError;					// Log errors on console?
	BOOL crashOnError;				// Raise an exceptionwhen an error occurs?
	int traceExecution;			// Trace SQL executions on console?
#endif
	int busyRetries;				// Number of retries to perform if database is busy
	NSMutableDictionary *columnInfo;
	ESStatement *beginImmediateTransactionStatement;
	ESStatement *beginExclusiveTransactionStatement;
	ESStatement *commitTransactionStatement;
	ESStatement *rollbackTransactionStatement;
#if SQLITE_VERSION_NUMBER >= 3005000
	int openFlags;					// Flags to pass to 'sqlite3_open_v2'
#endif
}

// Properties bound to a field
@property (nonatomic, readonly) sqlite3 *dbhandle;		// This is made available only for advanced uses... where there's no alternative with the API of ESDatabase
@property (nonatomic, readonly) NSString *name;			// DB name as passed by the client
@property (nonatomic, readonly) NSString *pathBundle;	// Path to the DB in the .app folder (not available for 'cache' DBs)
@property (nonatomic, readonly) NSString *pathLocal;	// Path to the DB on the device (not available for 'read-only' DBs)
@property (nonatomic, readonly) BOOL isReadOnly;		// Is this DB 'read-only' (read directly from .app folder, not modifiable)?
@property (nonatomic, readonly) BOOL isCache;			// Is this DB a 'cache' (created on the fly on the device)?
@property (nonatomic, readonly) BOOL isMemory;			// Is this DB in memory only (not persisted)?
@property (nonatomic, readonly) BOOL isAutoCommit;		// Is this database in 'auto-commit' mode currently (ie, no explicit transaction in progress)
@property (nonatomic, readonly) int changes;			// Number of rows modified by last statement
@property (nonatomic, assign) BOOL logError;			// Should errors be logged on console? (only in debug mode)
@property (nonatomic, assign) BOOL crashOnError;		// Generate a crash when an error occurs? (only in debug mode)
@property (nonatomic, assign) int traceExecution;		// Should all queries be logged on console (only in debug mode)
@property (nonatomic, assign) int busyRetries;			// Number of retries to perform if database is busy
#if SQLITE_VERSION_NUMBER >= 3005000
@property (nonatomic, assign) int openFlags;			// Flags to pass to 'sqlite3_open_v2'
#endif

// Virtual properties
@property (nonatomic, readonly) BOOL exists;			// Does a local file (on the device) exist for this DB?
@property (nonatomic, readonly) sqlite_int64 lastInsertRowId;
@property (nonatomic, readonly) NSString *lastErrorMessage;
@property (nonatomic, readonly) int lastErrorCode;
@property (nonatomic, readonly) BOOL hadError;

// Class functions
+ (ESDatabase *)newReadOnly:(NSString *)aName;		// New DB with 'aName' in read-only mode (this means the DB will be queried directly from the app folder, meaning it can't be modified)
+ (ESDatabase *)newReadWrite:(NSString *)aName;		// New DB with 'aName' in read-write mode (this means that a copy of the DB will made from the app folder to the iPhone's local folder)
+ (ESDatabase *)newCache:(NSString *)aName;			// New DB with 'aName' in read-write mode, no initial data is available, the database is to be created once on the fly using 'schema'
+ (ESDatabase *)newMemory;							// New 'in memory' DB (not persisted)
+ (NSString *)sqliteLibVersion;

// Instance functions
- (id)initReadOnly:(NSString *)aName;
- (id)initReadWrite:(NSString *)aName;
- (id)initCache:(NSString *)aName;
- (id)initMemory;

- (BOOL)open;									// Open DB from either pathLocal (for non-read-only DBs), or pathBundle (for read-only DBs)
- (BOOL)close;									// Close connection to DB
- (BOOL)setSchema:(NSArray *)aSchema;			// Set the schema of a cache DB, this is done only once, when cache DB is first created
- (BOOL)goodConnection;

- (ESStatement *)prepare:(NSString *)aQuery;
- (BOOL)execute:(NSString *)aQuery withArray:(NSArray *)args;
- (BOOL)execute:(NSString *)aQuery, ...;

- (ESResultSet *)select:(NSString *)aQuery withArray:(NSArray *)args;
- (ESResultSet *)select:(NSString *)aQuery, ...;

- (BOOL)beginTransaction;
- (BOOL)beginExclusiveTransaction;
- (BOOL)commit;
- (BOOL)rollback;

// Encryption, this will only work if you have purchased the sqlite encryption extensions.
- (BOOL)reKey:(NSString*)aKey;
- (BOOL)setKey:(NSString*)aKey;

// Reflection
- (BOOL)tableExists:(NSString *)aTableName;
- (NSString *)schema:(NSString *)aTableName;		// Schema for 'aTableName', or entire DB if 'aTableName' is nil
- (NSString *)schema;								// Schema for the DB (all tables)
- (ESColumnInfo *)columnInfo:(NSString *)aTableName columnName:(NSString *)aColumnName;	// Column info for 'aColumnName' in 'aTableName'

- (int)columnId:(NSString *)aTableName columnName:(NSString *)aColumnName;
- (NSString *)columnType:(NSString *)aTableName columnName:(NSString *)aColumnName;		// Column type for 'aColumnName' in 'aTableName'
- (BOOL)columnNullable:(NSString *)aTableName columnName:(NSString *)aColumnName;
- (BOOL)columnExists:(NSString *)aTableName columnName:(NSString *)aColumnName;			// Does column with 'aColumnName' exist in 'aTableName'?

@end

// -----------------------
// -- class ESStatement --
// -----------------------
@interface ESStatement : NSObject {
	sqlite3_stmt *sthandle;	// Underlying statement handle
	ESDatabase *database;	// Database object that created this statement
	NSString *query;		// SQL query
	int columnCount;		// Number of columns in 'query'
	int paramCount;			// Number of parameter placeholders in 'query'
	long hitCount;			// How many times this statement was executed
@private
	NSMutableDictionary *columnNameToIndexMap;	// Column-name to index hash
}

// Properties bound to a field
@property (nonatomic, readonly) sqlite3_stmt *sthandle;
@property (nonatomic, readonly) ESDatabase *database;
@property (nonatomic, readonly) NSString *query;
@property (nonatomic, readonly) int columnCount;
@property (nonatomic, readonly) int paramCount;
@property (nonatomic, readonly) long hitCount;
@property (nonatomic, readonly) int busyRetries;		// Number of retries to perform if database is busy

// Virtual properties
@property (nonatomic, readonly) NSString *description;
@property (nonatomic, readonly) BOOL logError;
@property (nonatomic, readonly) BOOL crashOnError;
@property (nonatomic, readonly) int traceExecution;

- (ESStatement *)initWithQuery:(NSString *)aQuery database:(ESDatabase *)aDatabase;
- (BOOL)close;									// Close statement, object can't be used anymore after this
- (void)reset;									// Reset an executed prepared statement

- (int)columnIndex:(NSString *)aName;

- (BOOL)executeWithArray:(NSArray *)args;		// Execute with arguments passed as an NSArray
- (BOOL)execute:(id)arg1, ...;					// Execute with a variable number of arguments
- (BOOL)execute;								// Execute without arguments

- (ESResultSet *)selectWithArray:(NSArray *)args;
- (ESResultSet *)select:(id)arg1, ...;
- (ESResultSet *)select;

@end


// -----------------------
// -- class ESResultSet --
// -----------------------
@interface ESResultSet : NSObject {
	ESStatement *statement;		// Statement object from which this result set comes from
}

// Properties bound to a field
@property (nonatomic, readonly) ESStatement *statement;
@property (nonatomic, readonly) int busyRetries;		// Number of retries to perform if database is busy

// Virtual properties
@property (nonatomic, readonly) ESDatabase *database;
@property (nonatomic, readonly) NSString *query;
@property (nonatomic, readonly) BOOL logError;
@property (nonatomic, readonly) BOOL crashOnError;
@property (nonatomic, readonly) int traceExecution;

- (ESResultSet *)initWithStatement:(ESStatement *)aStatement;

- (void)close;		// Close result set, object can't be used anymore after this
- (BOOL)next;

// Data retrieval by index
- (int)intValue:(int)aColumnIndex;
- (long)longValue:(int)aColumnIndex;
- (long long int)longLongIntValue:(int)aColumnIndex;
- (BOOL)boolValue:(int)aColumnIndex;
- (float)floatValue:(int)aColumnIndex;
- (double)doubleValue:(int)aColumnIndex;
- (NSString *)stringValue:(int)aColumnIndex;
- (NSDate *)dateValueWithTimeIntervalSince1970:(int)aColumnIndex;
- (NSDate *)dateValueCCYYMMDD:(int)aColumnIndex;					// Expected date of form CCYYMMD
- (NSDate *)dateValueCCYYMMDDdashed:(int)aColumnIndex;				// Expected date of form CCYY-MM-DD
- (NSDate *)dateValueCCYYMMDDslashed:(int)aColumnIndex;				// Expected date of form CCYY/MM/DD
- (NSData *)dataValue:(int)aColumnIndex;

// Data retrieval by column name
- (int)intValueByName:(NSString *)aColumnName;
- (long)longValueByName:(NSString *)aColumnName;
- (long long int)longLongIntValueByName:(NSString *)aColumnName;
- (BOOL)boolValueByName:(NSString *)aColumnName;
- (float)floatValueByName:(NSString *)aColumnName;
- (double)doubleValueByName:(NSString *)aColumnName;
- (NSString *)stringValueByName:(NSString *)aColumnName;
- (NSDate *)dateValueCCYYMMDDByName:(NSString *)aColumnName;
- (NSData *)dataValueByName:(NSString *)aColumnName;

@end
