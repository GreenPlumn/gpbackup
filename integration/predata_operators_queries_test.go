package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("GetOperators", func() {
		It("returns a slice of operators", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR ## (LEFTARG = bigint, PROCEDURE = numeric_fac)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR ## (bigint, NONE)")

			expectedOperator := backup.Operator{Oid: 0, Schema: "public", Name: "##", Procedure: "numeric_fac", LeftArgType: "bigint", RightArgType: "-", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}

			results := backup.GetOperators(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
		It("returns a slice of operators with a full-featured operator", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")

			testutils.AssertQueryRuns(connection, `CREATE FUNCTION testschema."testFunc"(path,path) RETURNS path AS 'SELECT $1' LANGUAGE SQL IMMUTABLE`)
			defer testutils.AssertQueryRuns(connection, `DROP FUNCTION testschema."testFunc"(path,path)`)

			testutils.AssertQueryRuns(connection, `
			CREATE OPERATOR testschema.## (
				LEFTARG = path,
				RIGHTARG = path,
				PROCEDURE = testschema."testFunc",
				COMMUTATOR = OPERATOR(testschema.##),
				NEGATOR = OPERATOR(public.###),
				RESTRICT = eqsel,
				JOIN = eqjoinsel,
				HASHES,
				MERGES
			)`)
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR testschema.## (path, path)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR ### (path, path)")

			version4expectedOperator := backup.Operator{Oid: 0, Schema: "testschema", Name: "##", Procedure: `testschema."testFunc"`, LeftArgType: "path", RightArgType: "path", CommutatorOp: "testschema.##", NegatorOp: "###", RestrictFunction: "eqsel", JoinFunction: "eqjoinsel", CanHash: true, CanMerge: false}
			expectedOperator := backup.Operator{Oid: 0, Schema: "testschema", Name: "##", Procedure: `testschema."testFunc"`, LeftArgType: "path", RightArgType: "path", CommutatorOp: "testschema.##", NegatorOp: "###", RestrictFunction: "eqsel", JoinFunction: "eqjoinsel", CanHash: true, CanMerge: true}

			results := backup.GetOperators(connection)

			Expect(len(results)).To(Equal(1))
			if connection.Version.Before("5") {
				testutils.ExpectStructsToMatchExcluding(&version4expectedOperator, &results[0], "Oid")
			} else {
				testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
			}
		})
		It("returns a slice of operators from a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR ## (LEFTARG = bigint, PROCEDURE = numeric_fac)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR ## (bigint, NONE)")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR testschema.## (LEFTARG = bigint, PROCEDURE = numeric_fac)")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR testschema.## (bigint, NONE)")
			backup.SetIncludeSchemas([]string{"testschema"})

			expectedOperator := backup.Operator{Oid: 0, Schema: "testschema", Name: "##", Procedure: "numeric_fac", LeftArgType: "bigint", RightArgType: "-", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}

			results := backup.GetOperators(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
	})
	Describe("GetOperatorFamilies", func() {
		BeforeEach(func() {
			testutils.SkipIf4(connection)
		})
		It("returns a slice of operator families", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testfam USING hash;")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testfam USING hash")

			expectedOperator := backup.OperatorFamily{Oid: 0, Schema: "public", Name: "testfam", IndexMethod: "hash"}

			results := backup.GetOperatorFamilies(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
		It("returns a slice of operator families in a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testfam USING hash;")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testfam USING hash")
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testschema.testfam USING hash;")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testschema.testfam USING hash")
			backup.SetIncludeSchemas([]string{"testschema"})

			expectedOperator := backup.OperatorFamily{Oid: 0, Schema: "testschema", Name: "testfam", IndexMethod: "hash"}

			results := backup.GetOperatorFamilies(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expectedOperator, &results[0], "Oid")
		})
	})
	Describe("GetOperatorClasses", func() {
		It("returns a slice of operator classes", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass FOR TYPE int USING hash AS STORAGE int")
			if connection.Version.Before("5") {
				defer testutils.AssertQueryRuns(connection, "DROP OPERATOR CLASS testclass USING hash")
			} else {
				defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testclass USING hash")
			}

			version4expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "", FamilyName: "", IndexMethod: "hash", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}
			expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			if connection.Version.Before("5") {
				testutils.ExpectStructsToMatchExcluding(&version4expected, &results[0], "Oid")
			} else {
				testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")
			}
		})
		It("returns a slice of operator classes with an operator family", func() {
			testutils.SkipIf4(connection)
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")

			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY testschema.testfam USING gist;")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testschema.testfam USING gist CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass FOR TYPE int USING gist FAMILY testschema.testfam AS STORAGE int")

			expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "testschema", FamilyName: "testfam", IndexMethod: "gist", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")
		})
		It("returns a slice of operator classes with different type and storage type", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")

			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass DEFAULT FOR TYPE int USING gist AS STORAGE text")
			if connection.Version.Before("5") {
				defer testutils.AssertQueryRuns(connection, "DROP OPERATOR CLASS testclass USING gist")
			} else {
				defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testclass USING gist")
			}

			version4expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "", FamilyName: "", IndexMethod: "gist", Type: "integer", Default: true, StorageType: "text", Operators: nil, Functions: nil}
			expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "gist", Type: "integer", Default: true, StorageType: "text", Operators: nil, Functions: nil}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			if connection.Version.Before("5") {
				testutils.ExpectStructsToMatchExcluding(&version4expected, &results[0], "Oid")
			} else {
				testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")
			}
		})
		It("returns a slice of operator classes with operators and functions", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass FOR TYPE int USING gist AS OPERATOR 1 = RECHECK, OPERATOR 2 < , FUNCTION 1 abs(integer), FUNCTION 2 int4out(integer)")
			if connection.Version.Before("5") {
				defer testutils.AssertQueryRuns(connection, "DROP OPERATOR CLASS testclass USING gist")
			} else {
				defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testclass USING gist")
			}

			expectedOperators := []backup.OperatorClassOperator{{ClassOid: 0, StrategyNumber: 1, Operator: "=(integer,integer)", Recheck: true}, {ClassOid: 0, StrategyNumber: 2, Operator: "<(integer,integer)", Recheck: false}}
			expectedFunctions := []backup.OperatorClassFunction{{ClassOid: 0, SupportNumber: 1, FunctionName: "abs(integer)"}, {ClassOid: 0, SupportNumber: 2, FunctionName: "int4out(integer)"}}
			version4expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "", FamilyName: "", IndexMethod: "gist", Type: "integer", Default: false, StorageType: "-", Operators: expectedOperators, Functions: expectedFunctions}
			expected := backup.OperatorClass{Oid: 0, Schema: "public", Name: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "gist", Type: "integer", Default: false, StorageType: "-", Operators: expectedOperators, Functions: expectedFunctions}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			if connection.Version.Before("5") {
				testutils.ExpectStructsToMatchExcluding(&version4expected, &results[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
			} else {
				testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
			}
		})
		It("returns a slice of operator classes for a specific schema", func() {
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testclass FOR TYPE int USING hash AS STORAGE int")
			if connection.Version.Before("5") {
				defer testutils.AssertQueryRuns(connection, "DROP OPERATOR CLASS testclass USING hash")
			} else {
				defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testclass USING hash")
			}
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema CASCADE")
			testutils.AssertQueryRuns(connection, "CREATE OPERATOR CLASS testschema.testclass FOR TYPE int USING hash AS STORAGE int")
			if connection.Version.Before("5") {
				defer testutils.AssertQueryRuns(connection, "DROP OPERATOR CLASS testschema.testclass USING hash")
			} else {
				defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY testschema.testclass USING hash")
			}
			backup.SetIncludeSchemas([]string{"testschema"})

			version4expected := backup.OperatorClass{Oid: 0, Schema: "testschema", Name: "testclass", FamilySchema: "", FamilyName: "", IndexMethod: "hash", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}
			expected := backup.OperatorClass{Oid: 0, Schema: "testschema", Name: "testclass", FamilySchema: "testschema", FamilyName: "testclass", IndexMethod: "hash", Type: "integer", Default: false, StorageType: "-", Operators: nil, Functions: nil}

			results := backup.GetOperatorClasses(connection)

			Expect(len(results)).To(Equal(1))
			if connection.Version.Before("5") {
				testutils.ExpectStructsToMatchExcluding(&version4expected, &results[0], "Oid")
			} else {
				testutils.ExpectStructsToMatchExcluding(&expected, &results[0], "Oid")
			}
		})
	})
})
