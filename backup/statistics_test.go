package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/lib/pq"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/statistics tests", func() {
	Describe("PrintStatisticsStatementsForTable", func() {
		var attStats []backup.AttributeStatistic
		var tupleStats backup.TupleStatistic
		BeforeEach(func() {
			attStats = []backup.AttributeStatistic{}
			tupleStats = backup.TupleStatistic{}
			toc, backupfile = testutils.InitializeTestTOC(buffer, "statistics")
		})
		It("prints tuple and attribute stats for single table with no stats", func() {
			tableTestTable := backup.BasicRelation("testschema", "testtable")
			tupleStats = backup.TupleStatistic{Schema: "testschema", Table: "testtable"}
			attStats = []backup.AttributeStatistic{}
			backup.PrintStatisticsStatementsForTable(backupfile, toc, tableTestTable, attStats, tupleStats)
			testutils.ExpectEntry(toc.StatisticsEntries, 0, "testschema", "testtable", "STATISTICS")
			testutils.AssertBufferContents(toc.StatisticsEntries, buffer, `UPDATE pg_class
SET
	relpages = 0::int,
	reltuples = 0.000000::real
WHERE relname = 'testtable'
AND relnamespace = 0;`)
		})
		It("prints tuple and attribute stats for single table with stats", func() {
			tableTestTable := backup.BasicRelation("testschema", "testtable")
			tupleStats = backup.TupleStatistic{Schema: "testschema", Table: "testtable"}
			attStats = []backup.AttributeStatistic{
				{Schema: "testschema", Table: "testtable", AttName: "testattWithArray", Type: "_array"},
				{Schema: "testschema", Table: "testtable", AttName: "testatt", Type: "_array", Relid: 2, AttNumber: 3, NullFraction: .4,
					Width: 10, Distinct: .5, Kind1: 20, Operator1: 10, Numbers1: pq.StringArray([]string{"1", "2", "3"}), Values1: pq.StringArray([]string{"4", "5", "6"})},
			}
			backup.PrintStatisticsStatementsForTable(backupfile, toc, tableTestTable, attStats, tupleStats)
			testutils.ExpectEntry(toc.StatisticsEntries, 0, "testschema", "testtable", "STATISTICS")
			testutils.AssertBufferContents(toc.StatisticsEntries, buffer, `UPDATE pg_class
SET
	relpages = 0::int,
	reltuples = 0.000000::real
WHERE relname = 'testtable'
AND relnamespace = 0;


DELETE FROM pg_statistic WHERE starelid = 'testschema.testtable'::regclass::oid AND staattnum = 0;

INSERT INTO pg_statistic VALUES (
	'testschema.testtable'::regclass::oid,
	0::smallint,
	0.000000::real,
	0::integer,
	0.000000::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL,
	NULL,
	NULL,
	NULL
);


DELETE FROM pg_statistic WHERE starelid = 'testschema.testtable'::regclass::oid AND staattnum = 3;

INSERT INTO pg_statistic VALUES (
	'testschema.testtable'::regclass::oid,
	3::smallint,
	0.400000::real,
	10::integer,
	0.500000::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL,
	NULL,
	NULL,
	NULL
);`)
		})
	})
	Describe("GenerateTupleStatisticsQuery", func() {
		It("generates tuple statistics query with a single quote in the table name", func() {
			tableTestTable := backup.BasicRelation("testschema", `"test'table"`)
			tupleStats := backup.TupleStatistic{Schema: "testschema", Table: `"test'table"`}
			tupleQuery := backup.GenerateTupleStatisticsQuery(tableTestTable, tupleStats)
			Expect(tupleQuery).To(Equal(`UPDATE pg_class
SET
	relpages = 0::int,
	reltuples = 0.000000::real
WHERE relname = '"test''table"'
AND relnamespace = 0;`))
		})

	})
	Describe("GenerateAttributeStatisticsQuery", func() {
		tableTestTable := backup.BasicRelation("testschema", `"test'table"`)
		attStats := backup.AttributeStatistic{Schema: "testschema", Table: "testtable", AttName: "testatt", Type: "", Relid: 2,
			AttNumber: 3, NullFraction: .4, Width: 10, Distinct: .5, Kind1: 20, Operator1: 10,
			Numbers1: pq.StringArray([]string{"1", "2", "3"}), Values1: pq.StringArray([]string{"4", "5", "6"})}
		It("generates attribute statistics query for array type", func() {
			attStats.Type = "_array"
			attStatsQuery := backup.GenerateAttributeStatisticsQuery(tableTestTable, attStats)
			Expect(attStatsQuery).To(Equal(`DELETE FROM pg_statistic WHERE starelid = 'testschema."test''table"'::regclass::oid AND staattnum = 3;

INSERT INTO pg_statistic VALUES (
	'testschema."test''table"'::regclass::oid,
	3::smallint,
	0.400000::real,
	10::integer,
	0.500000::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL,
	NULL,
	NULL,
	NULL
);`))
		})
		It("generates attribute statistics query for non-array type", func() {
			attStats.Type = "testtype"
			attStatsQuery := backup.GenerateAttributeStatisticsQuery(tableTestTable, attStats)
			Expect(attStatsQuery).To(Equal(`DELETE FROM pg_statistic WHERE starelid = 'testschema."test''table"'::regclass::oid AND staattnum = 3;

INSERT INTO pg_statistic VALUES (
	'testschema."test''table"'::regclass::oid,
	3::smallint,
	0.400000::real,
	10::integer,
	0.500000::real,
	20::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	10::oid,
	0::oid,
	0::oid,
	0::oid,
	'{"1","2","3"}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	array_in('{"4","5","6"}', 'testtype'::regtype::oid, -1),
	NULL,
	NULL,
	NULL
);`))
		})
	})
	Describe("AnyValues", func() {
		It("returns properly casted string when length of anyvalues is greater than 0", func() {
			castedString := backup.AnyValues(pq.StringArray([]string{"1", "2"}), "int")
			Expect(castedString).To(Equal(`array_in('{"1","2"}', 'int'::regtype::oid, -1)`))
		})
		It("returns NULL if anyvalues is of length 0", func() {
			castedString := backup.AnyValues(pq.StringArray([]string{}), "int")
			Expect(castedString).To(Equal(`NULL`))
		})
	})
	Describe("SliceToPostgresArray", func() {
		It("returns properly quoted string representing a Postgres array", func() {
			arrayString := backup.SliceToPostgresArray([]string{"ab'c", `ef"g`})
			Expect(arrayString).To(Equal(`'{"ab''c","ef\"g"}'`))
		})
	})
})
