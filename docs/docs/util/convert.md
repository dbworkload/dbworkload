# convert

## Convert Oracle PL to CockroachDB PL

This is an **experimental feature**.

### Installation

You need to install the `convert` package which adds additional libraries.

```bash
pip install dbworkload[convert]
```

### How it works

The `convert` command creates an AI Agent powered by  [LangChain](https://docs.langchain.com/) and [LangGraph](https://docs.langchain.com/oss/python/langgraph/overview) to efficiently interacting with the LLM of your choice.

Additionally, the agent uses **RAG**, powered by CockroachDB as the Vector database.

#### LangGraph cycle overview

The cycle - the series of steps taken by the AI Agent - can be summarized in below table.

| Node Name       | LangChain Component                  | Function in the Workflow |
| --------------- | ------------------------------------ | ------------------------ |
| 1. retriever    | Retriever (CockroachDB Vector Store) | Fetches the most semantically similar past Oracle/CockroachDB translation pairs. |
| 2. generator    | LLMChain (Translator LLM)            | Creates the first draft of the CockroachDB procedure, grounded by the retrieved examples (RAG). |
| 3. validator    | Custom Tool (CockroachDB Execution)  | Runs the drafted code against a live CockroachDB environment. Returns 'PASS' or 'FAIL' + the database error/critique. If success, routes to the `indexer`, else to the `refiner` - up to 3 times |
| 4. refiner      | LLMChain (Refiner LLM)               | Receives the failed code and the critique from the `validator`. It attempts to fix the draft code, then routes back to the `validator` node |
| 5. indexer      | Custom Tool (Vector Store Insert)    | Runs ONLY on success. Takes the final, validated Oracle/CockroachDB pair and adds it to the vector database for future learning. |
| 6. final_output | Custom Function/Tool                 | Saves the final, validated CockroachDB procedure to the output file system. |

#### Input Directory

`convert` expects to find the data to be converted in the specified directory's `in/` directory.

There should be 3 files for every Stored Procedure or Function you want to convert, all sharing the same _basename_ but with different file extensions.

Example, if the basename is `001`:

```bash
in/001.ddl   # <-- the Oracle code
in/001.sql   # <-- the seeding and test statements
in/001.json  # <-- the expected output for each of the test statements
```

#### Retriever Node

This Node reads the source PL code in the `.ddl` file and attempts to find similar,
previously converted Oracle-CockroachDB conversion from
the Vector Store with the same PL code.

You need to make a CockroachDB cluster available and create, if it doesn't exist already, the following table.

```sql
CREATE TABLE defaultdb.queries (
    src_crc32 INT8 NOT NULL,
    query_combo STRING NULL,
    src_query_embedding VECTOR(384) NULL,
    CONSTRAINT pk PRIMARY KEY (src_crc32 ASC),
    VECTOR INDEX vector_idx (src_query_embedding vector_l2_ops)
);
```

Converstion that are validated will be added to the database by the Indexer Node.

#### Generator Node

The role of this node is to query the LLM providing a robust prompt
and the similar conversions from the Vector store,
as retrieved in the previous node.

You have the capability to specify a LLM for the Generate Node, and a separate LLM for the Refiner Node.

#### Validator Node

With the converted draft received from the LLM, the Validator will attempt the following steps:

- **seed** the target CockroachDB cluster (the details are specified in the `.sql` file),
- execute the **DDL** (the converted code) and validate the statements completes successfully,
- Run the **tests** and compare the actual result with the expected result in the `.json` file.

If all these steps succeed, the flow of execution is routed to the Indexer Node, else to the Refiner Node.

#### Refiner Node

The Refiner node takes the error message returned by CockroachDB and queries the LLM again for a corrected version.

You can specify a separate LLM engine for the Refiner.

Upon receipt of the new code, the flow is returned to the Validator Node for a 2nd attempt.

This cycle can repeat up to 3 times, then the Agent gives up and returns the error.

#### Indexer Node

If the conversion was successful, the source PL code and the Target CockroachDB PL/pgSQL code is saved in the Vector database for use by the Retrieve Node.

#### Final Output

When the cycle completes, files are written to the `out/` directory.

The below files will be written:

```bash
out/001.ai.yaml  # <-- The full interaction with the LLM
out/001.ddl      # <-- The converted PL code
out/001.json     # <-- The actual output from running the tests
out/001.out      # <-- THe log file
```

### Example

You need 3 files for each PL routine.

File `test.ddl`

```sql
CREATE OR REPLACE PROCEDURE get_students_by_department
AS
BEGIN
    SELECT * FROM students
    WHERE department = p_department;
END;
```

File `test.sql`

```sql
-- this file has 3 sections, delimited by the `betwixt_*` tag.

-- SECTION 1: seeding files
-- list here all the files, one file per line, and make sure they all end with `.sql`
seed.sql

-- betwixt_file_end 
-- ^^^^^^^^^^^^^^^^ this is what marks the end of the file section 

-- SECTION 2: seeding statements
-- list here all your individual seeding sql statements.
-- make sure they are all separated by a semi-colon.

select 'this is a silly seeding sql statement!';

select 
   version(),
   now(),
;

-- betwixt_seed_end
-- ^^^^^^^^^^^^^^^^ this is what marks the end of the seeding statements section

-- SECTION 3: test statements

-- test 0
SELECT 
    version();

-- test 1
CALL 
get_students_by_department('Computer Science');

--  test 2
CALL 
get_students_by_department('boooooh!!');

-- test 3
CALL 
get_students_by_department();

--test 4
SELECT * 
FROM students;
```

File `test.json`

```json
{
  "0": [
    {
      "version": "CockroachDB CCL v25.2.2 (x86_64-apple-darwin19, built 2025/06/23 13:45:24, go1.23.7 X:nocoverageredesign)"
    }
  ],
  "1": [],
  "2": [],
  "3": "ERR",
  "4": [
    {
      "student_id": 1,
      "first_name": "John",
      "last_name": "Doe",
      "department": "Computer Science",
      "age": 20
    },
    {
      "student_id": 2,
      "first_name": "Jane",
      "last_name": "Smith",
      "department": "Mathematics",
      "age": 22
    },
    {
      "student_id": 3,
      "first_name": "Michael",
      "last_name": "Johnson",
      "department": "Biology",
      "age": 21
    },
    {
      "student_id": 4,
      "first_name": "Emily",
      "last_name": "Brown",
      "department": "Physics",
      "age": 23
    },
    {
      "student_id": 5,
      "first_name": "David",
      "last_name": "Wilson",
      "department": "Chemistry",
      "age": 20
    },
    {
      "student_id": 6,
      "first_name": "Sarah",
      "last_name": "Lee",
      "department": "Computer Science",
      "age": 22
    }
  ]
}
```

The test statements file, `test.sql` points to the `seed.sql` file.

File: `seed.sql`

```sql
use defaultdb;
drop database if exists ora2crdb;
create database ora2crdb;
use ora2crdb;


CREATE TABLE students (
    student_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50),
    age INT
);

INSERT INTO students (student_id, first_name, last_name, department, age)
VALUES 
    (1, 'John', 'Doe', 'Computer Science', 20),
    (2, 'Jane', 'Smith', 'Mathematics', 22),
    (3, 'Michael', 'Johnson', 'Biology', 21),
    (4, 'Emily', 'Brown', 'Physics', 23),
    (5, 'David', 'Wilson', 'Chemistry', 20),
    (6, 'Sarah', 'Lee', 'Computer Science', 22);
```

Now that we have all required files, you can attempt the conversion.

!!! Note "LLMs are not deterministic by design!"
    The result will vary as LLMs are refreshed and updated regularly.
    Also, LLMs are not deterministic so your results will vary from run to run.
    This is expected behavior from the LLM.

```bash
# since we're using a OpenAI model, we need to make sure we export the API KEY
export OPENAI_API_KEY=xxxyyyzzz

# the in/ directory is present in the current director, `.`
# You don't have to use public LLMs: you can use Ollama and run your
# local, private llm in a completely air-gapped environment.
dbworkload util convert \
  --uri 'postgres://user:pass@localhost:26257/defaultdb?sslmode=require' \
  --dir .  \
  --generator-llm ollama:llama3.2:3b \
  --refiner-llm OpenAI:gpt-5
```

In this experimental version, only `ollama` and `openai` models are supported.
