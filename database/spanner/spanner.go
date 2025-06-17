package spanner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	nurl "net/url"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	sdb "cloud.google.com/go/spanner/admin/database/apiv1"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/spanner/ddl"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/hashicorp/go-multierror"
	"github.com/samber/lo"
	"google.golang.org/api/iterator"
)

func init() {
	db := Spanner{}
	database.Register("spanner", &db)
}

// DefaultMigrationsTable is used if no custom table is specified
const DefaultMigrationsTable = "SchemaMigrations"

// Driver errors
var (
	ErrNilConfig      = errors.New("no config")
	ErrNoDatabaseName = errors.New("no database name")
	ErrNoSchema       = errors.New("no schema")
	ErrDatabaseDirty  = errors.New("database is dirty")
	ErrLockHeld       = errors.New("unable to obtain lock")
	ErrLockNotHeld    = errors.New("unable to release already released lock")
)

// Config used for a Spanner instance
type Config struct {
	MigrationsTable string
	DatabaseName    string
}

// Spanner implements database.Driver for Google Cloud Spanner
type Spanner struct {
	db *DB

	config *Config
}

type DB struct {
	admin *sdb.DatabaseAdminClient
	data  *spanner.Client
	lock  DistributedLock
}

type Version struct {
	Version  int
	Dirty    bool
	Checksum string
	Lock     DistributedLock
}

func NewDB(admin *sdb.DatabaseAdminClient, data *spanner.Client) *DB {
	return &DB{
		admin: admin,
		data:  data,
		lock:  newDistributedLock(),
	}
}

// WithInstance implements database.Driver
func WithInstance(instance *DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if len(config.DatabaseName) == 0 {
		return nil, ErrNoDatabaseName
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	sx := &Spanner{
		db:     instance,
		config: config,
	}

	if err := sx.ensureVersionTable(); err != nil {
		return nil, err
	}

	return sx, nil
}

// Open implements database.Driver
func (s *Spanner) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	adminClient, err := sdb.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	dbname := strings.Replace(migrate.FilterCustomQuery(purl).String(), "spanner://", "", 1)
	dataClient, err := spanner.NewClient(ctx, dbname)
	if err != nil {
		log.Fatal(err)
	}

	migrationsTable := purl.Query().Get("x-migrations-table")

	db := NewDB(adminClient, dataClient)

	return WithInstance(db, &Config{
		DatabaseName:    dbname,
		MigrationsTable: migrationsTable,
	})
}

// Close implements database.Driver
func (s *Spanner) Close() error {
	s.db.data.Close()
	return s.db.admin.Close()
}

// Lock implements a common distributed lock for Spanner.
// Wait until the lock is available. The lock will be available if not help by another process, or the TTL of the current lock has expired.
func (s *Spanner) Lock() error {
	// Get the row for the lock, if it exists.
	ver, err := s.version()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to get current version"}
	}

	if ver == nil {
		ver = &Version{
			Version:  -1,
			Dirty:    false,
			Checksum: "",
			Lock:     s.db.lock,
		}
	}

	if ver.Lock == s.db.lock {
		go func() {
			tick := time.NewTicker(5 * time.Second)
			defer tick.Stop()

			for {

			}
		}()
	}

	expired := ver.Lock.Expired()

	if expired {

	}

	_, err = s.db.data.ReadWriteTransaction(context.Background(), func(ctx context.Context, rwt *spanner.ReadWriteTransaction) error {
		rwt.Update(ctx, spanner.Statement{
			SQL: `INSERT INTO `,
		})

		return nil
	})
	if err != nil {
		return fmt.Errorf("%w failed to lock spanner db: %w", ErrLockHeld, err)
	}

	return nil
}

// Unlock implements database.Driver but no action required, see Lock.
func (s *Spanner) Unlock() error {
	return nil
}

// Run implements database.Driver
func (s *Spanner) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	stmts, err := ddl.ToMigrationStatements("", string(migr))
	if err != nil {
		return &database.Error{OrigErr: err, Err: "failed to cleanup statements", Query: migr}
	}

	ctx := context.Background()

	// Because the statements limit is 10, we have to chunk the statements.
	for i, chunk := range lo.Chunk(stmts, 10) {
		// The migration is usually very slow, some can take 10 more minutes to run.
		// Print some progress so that it doesn't look like the process is stuck.
		slog.InfoContext(ctx, "spanner db migration progress", "progress", i*10+len(chunk), "total", len(stmts))

		op, err := s.db.admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database:   s.config.DatabaseName,
			Statements: chunk,
		})
		if err != nil {
			return &database.Error{OrigErr: err, Err: "failed to update spanner db ddl", Query: []byte(strings.Join(chunk, "; "))}
		}

		if err := op.Wait(ctx); err != nil {
			return &database.Error{OrigErr: err, Err: "failed to wait for spanner db ddl update", Query: []byte(strings.Join(chunk, "; "))}
		}
	}

	return nil
}

// SetVersion implements database.Driver
func (s *Spanner) SetVersion(version int, dirty bool) error {
	s.setVersion(&Version{
		Version:  version,
		Dirty:    dirty,
		Checksum: "",
		Lock:     "",
	})

	return nil
}

// Version implements database.Driver
func (s *Spanner) Version() (int, bool, error) {
	ver, err := s.version()
	if err != nil {
		return 0, false, &database.Error{OrigErr: err}
	}

	if ver == nil || ver.Version < 0 {
		return database.NilVersion, false, nil
	}

	return ver.Version, ver.Dirty, nil
}

var nameMatcher = regexp.MustCompile(`(CREATE TABLE\s(\S+)\s)|(CREATE.+INDEX\s(\S+)\s)`)

// Drop implements database.Driver. Retrieves the database schema first and
// creates statements to drop the indexes and tables accordingly.
// Note: The drop statements are created in reverse order to how they're
// provided in the schema. Assuming the schema describes how the database can
// be "build up", it seems logical to "unbuild" the database simply by going the
// opposite direction. More testing
func (s *Spanner) Drop() error {
	ctx := context.Background()
	res, err := s.db.admin.GetDatabaseDdl(ctx, &adminpb.GetDatabaseDdlRequest{
		Database: s.config.DatabaseName,
	})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "drop failed"}
	}
	if len(res.Statements) == 0 {
		return nil
	}

	stmts := make([]string, 0)
	for i := len(res.Statements) - 1; i >= 0; i-- {
		s := res.Statements[i]
		m := nameMatcher.FindSubmatch([]byte(s))

		if len(m) == 0 {
			continue
		} else if tbl := m[2]; len(tbl) > 0 {
			stmts = append(stmts, fmt.Sprintf(`DROP TABLE %s`, tbl))
		} else if idx := m[4]; len(idx) > 0 {
			stmts = append(stmts, fmt.Sprintf(`DROP INDEX %s`, idx))
		}
	}

	op, err := s.db.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   s.config.DatabaseName,
		Statements: stmts,
	})
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(strings.Join(stmts, "; "))}
	}
	if err := op.Wait(ctx); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(strings.Join(stmts, "; "))}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Spanner type.
func (s *Spanner) ensureVersionTable() (err error) {
	if err = s.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := s.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	ctx := context.Background()
	tbl := s.config.MigrationsTable
	iter := s.db.data.Single().Read(ctx, tbl, spanner.AllKeys(), []string{"Version"})
	if err := iter.Do(func(r *spanner.Row) error { return nil }); err == nil {
		return nil
	}

	stmt := fmt.Sprintf(`CREATE TABLE %s (
		Version  INT64 NOT NULL,
		Dirty    BOOL NOT NULL,
		Checksum STRING(MAX),
		Lock     STRING(MAX)
	) PRIMARY KEY(Version)`, tbl)

	op, err := s.db.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   s.config.DatabaseName,
		Statements: []string{stmt},
	})

	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}
	if err := op.Wait(ctx); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(stmt)}
	}

	return nil
}

// return nil if not found
func (s *Spanner) version() (*Version, error) {
	ctx := context.Background()
	stmt := spanner.Statement{
		SQL: `SELECT Version, Dirty, Checksum, Lock FROM ` + s.config.MigrationsTable + ` LIMIT 1`,
	}
	iter := s.db.data.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		if err == iterator.Done {
			return nil, nil // No version found, return empty version
		}
		return nil, &database.Error{OrigErr: err, Query: []byte(stmt.SQL)}
	}

	var v Version
	if err := row.Columns(&v.Version, &v.Dirty, &v.Checksum, &v.Lock); err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(stmt.SQL)}
	}

	return &v, nil
}

func (s *Spanner) setVersion(v *Version) error {
	ctx := context.Background()

	_, err := s.db.data.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutations := []*spanner.Mutation{
			spanner.Delete(s.config.MigrationsTable, spanner.AllKeys()),
			spanner.Insert(s.config.MigrationsTable,
				[]string{"Version", "Dirty", "Checksum", "Lock"},
				[]interface{}{v.Version, v.Dirty, v.Checksum, v.Lock}),
		}
		return txn.BufferWrite(mutations)
	})

	if err != nil {
		return &database.Error{OrigErr: err}
	}

	return nil
}
