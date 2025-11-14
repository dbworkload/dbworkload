import json
import logging
import os
from typing import Any, TypedDict

import openai
import psycopg
import sqlparse
import yaml
from langchain_core.callbacks import get_usage_metadata_callback
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from psycopg.rows import dict_row

from .prompts import REFINER_PROMPT, SYSTEM_PROMPT

# setup global logger
logger = logging.getLogger("dbworkload")

openai.api_key = os.getenv("OPENAI_API_KEY")  # or set it directly if needed


def get_llm(provider: str, model: str) -> ChatOllama | ChatOpenAI | None:

    if provider.lower() == "openai":
        return ChatOpenAI(
            model=model,
            temperature=0.1,
        )

    if provider.lower() == "ollama":
        return ChatOllama(
            model=model,
            temperature=0.1,
        )


class ConversionState(TypedDict):
    """The state shared across all nodes in the graph."""

    oracle_code: str  # Initial input code
    converted_code: str  # The current version of the code
    validation_error: str  # Error message from the validator
    history: list  # Log of attempts and errors
    max_attempts: int  # Stop condition
    attempts: int


class ConvertTool:
    def __init__(
        self,
        base_dir: str,
        uri: str,
        root: str,
        generator_llm: str,
        refiner_llm: str,
    ):

        self.base_dir = base_dir
        self.uri = uri
        self.root = root

        self.expected_output: dict = {}
        self.seed_statements: list[str] = []
        self.test_statements: list[str] = []
        self.generator_llm_provider, self.generator_llm_model = generator_llm.split(
            ":", maxsplit=1
        )
        self.refiner_llm_provider, self.refiner_llm_model = refiner_llm.split(
            ":", maxsplit=1
        )

        # Create the Prompt Template
        self.prompt = ChatPromptTemplate.from_messages(
            [("system", SYSTEM_PROMPT), ("user", "{oracle_code}")]
        )

        # Initialize your LLM (using a powerful model is key for code translation)
        self.refiner_llm = get_llm(*refiner_llm.split(":", maxsplit=1))

        self.generator_llm = get_llm(*generator_llm.split(":", maxsplit=1))

        self.parser = StrOutputParser()

        # Create the runnable chain
        self.conversion_chain = (
            {
                "oracle_code": RunnablePassthrough(),
            }
            | self.prompt
            | self.generator_llm
            | self.parser
        )

        self.refiner_chain = (
            ChatPromptTemplate.from_messages(
                [
                    ("system", REFINER_PROMPT),
                    (
                        "user",
                        "Please provide the fixed, corrected CockroachDB PL/pgSQL code.",
                    ),
                ]
            )
            | self.refiner_llm
            | self.parser
        )

    def refine(self, state: ConversionState) -> dict:

        logger.info(f"⚙️  Refiner Node (Attempt #{state['attempts'] + 1})")

        logger.info(f"📡 ➡️  Sending query to {self.refiner_llm_model}")
        with get_usage_metadata_callback() as ctx:
            refined_code = self.refiner_chain.invoke(
                {
                    "validation_error": state["validation_error"],
                    "converted_code": state["converted_code"],
                }
            )

            logger.info(
                f"📡 ⬅️  Receiving from {self.refiner_llm_model}: {refined_code=}"
            )

            logger.info(f"💰 {self.refiner_llm_model} cost={ctx.usage_metadata}")

        # 3. Update the state with the refined code for the next loop's generator
        return {
            "converted_code": refined_code,  # Pass the refined code back to the validator for the next pass
            "validation_error": "",  # Clear the error for the next attempt
            "history": state.get("history", [])
            + [
                {
                    "attempt": state.get("attempts"),
                    "status": "Refined",
                    "refined_code": refined_code,
                }
            ],
        }

    def generate_code(self, state: ConversionState) -> dict:
        """
        Invokes the LangChain conversion pipeline to generate the first draft
        or a refined draft of the CockroachDB PL/pgSQL code.
        """

        logger.info(f"⚙️  Generate Node (Attempt #{state['attempts'] + 1})")

        # 1. Get the Oracle code from the current state
        oracle_code = state["oracle_code"]

        # 2. Execute the LangChain Runnable (the pipeline)
        # The 'invoke' command sends the code to the LLM via the API.
        logger.info(f"📡 ➡️  Sending query to {self.generator_llm_model}")

        with get_usage_metadata_callback() as ctx:
            converted_code_output = self.conversion_chain.invoke(oracle_code)

            logger.info(
                f"📡 ⬅️  Receiving from {self.generator_llm_model}: {converted_code_output=}"
            )

            logger.info(
                f"💰 {self.generator_llm_model} cost={ctx.usage_metadata[self.generator_llm.model]}"
            )

        # 3. Update the state for the next node in the graph
        # We also increment the attempt counter

        return {
            "converted_code": converted_code_output,
            "history": state.get("history", [])
            + [
                {
                    "attempt": state.get("attempts"),
                    "code": converted_code_output,
                    "status": "Generated",
                }
            ],
        }

    def validate_code(self, state: ConversionState) -> dict:
        """
        Checks the converted code for syntax or logical errors.
        Returns the error message if a failure is found, or an empty string on success.
        """
        logger.info(f"⚙️  Validator Node (Attempt #{state['attempts'] + 1})")

        converted_code = state["converted_code"]

        if self.seed_statements:
            logger.info(f"🌱 Seeding CockroachDB prior to running tests")

            self.execute_sql_stmts(self.seed_statements)

        logger.info("Creating the CockroachDB SP in the test cluster")

        try:
            with psycopg.connect(self.uri, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(converted_code)

                    logger.info(f"🪳 🟢 {cur.statusmessage}")

        except Exception as e:

            error_message = str(e)
            logger.error(f"🪳 🔴 {error_message}")

            with open(f"{self.base_dir}/out/{self.root}.out", "a") as f:
                f.write("Error creating Stored Procedure\n")
                f.write(error_message)
                f.write("\n\n")

            new_attempts = state.get("attempts", 0) + 1
            return {
                "validation_error": error_message,
                "attempts": new_attempts,
                "history": state.get("history", [])
                + [
                    {
                        "attempt": state["attempts"],
                        "status": "Validated",
                        "error": error_message,
                    }
                ],
            }

        logger.info("Run the SQL Test statements against the CockroachDB cluster")

        actual: dict = {}
        error_message = ""

        for idx, s in enumerate(self.test_statements):
            try:

                # Connect; row_factory yields dicts keyed by column names
                with psycopg.connect(self.uri, autocommit=True) as conn:

                    with conn.cursor(row_factory=dict_row) as cur:
                        cur.execute(s)

                        # If cursor.description is present, we have a result set (e.g., SELECT/SHOW)
                        if cur.description is not None:
                            actual[idx] = [
                                {k: self.to_jsonable(v) for k, v in row.items()}
                                for row in cur
                            ]

                with open(f"{self.base_dir}/out/{self.root}.out", "a") as f:
                    if actual[idx] == self.expected_output[str(idx)]:
                        logger.info(f"{idx=} : 🟢 OK")
                        f.write(f"{idx=} : 🟢 OK ")
                    else:
                        logger.info(f"{idx=} : 🔴 FAIL")
                        f.write(f"{idx=} : 🔴 FAIL")

                    f.write("\n")

            except Exception as e:
                error_message = str(e)

                with open(f"{self.base_dir}/out/{self.root}.out", "a") as f:
                    if "ERR" == self.expected_output[str(idx)]:
                        # The ERR in this case is expected, so it is a success
                        logger.info(f"{idx=} : 🟢 OK")
                        f.write(f"{idx=} : 🟢 OK ")
                        error_message = ""
                    else:
                        logger.info(f"{idx=} : 🔴 FAIL")
                        f.write(f"{idx=} : 🔴 FAIL")

                        f.write("<SQL>\n")
                        f.write(s)
                        f.write("\n</SQL>\n")
                        f.write(f"<Error message>\n")
                        f.write(error_message)
                        f.write("\n</Error message>\n")
                    f.write("\n")

            with open(f"{self.base_dir}/out/{self.root}.json", "w") as f:
                f.write(json.dumps(actual, indent=4))

        new_attempts = state.get("attempts", 0) + 1
        return {
            "validation_error": error_message,
            "attempts": new_attempts,
            "history": state.get("history", [])
            + [
                {
                    "attempt": state["attempts"],
                    "status": "Validated",
                    "error": error_message,
                }
            ],
        }

    def langgraph_it(self, oracle_sp: str):
        # Conceptual Graph Structure

        graph_builder = StateGraph(ConversionState)

        # 1. Start with the Generator
        graph_builder.add_node("generator", self.generate_code)

        # 2. After generation, always validate
        graph_builder.add_node("validator", self.validate_code)
        graph_builder.add_edge("generator", "validator")

        # 3. Add the Refiner node
        graph_builder.add_node("refiner", self.refine)

        # 4. Define the conditional edge (The Loop)
        def should_continue(state: ConversionState):
            if state["validation_error"]:
                if state["attempts"] >= 3:
                    # max attempts reached
                    logger.error(
                        f"❌ Failed conversion for {self.root}: max attempt reached."
                    )
                    return "end"
                # Code failed validation
                logger.warning(f"⚠️  Validation failed. Re-attempt conversion...")
                return "refiner"
            else:
                # Success: end the graph
                logger.info(f"✅ Successful conversion for {self.root}")
                return "end"

        graph_builder.add_conditional_edges(
            "validator",  # Start the condition check from the validator node
            should_continue,  # The function that makes the decision
            {"refiner": "refiner", "end": END},
        )

        # 5. Complete the loop
        graph_builder.add_edge("refiner", "validator")
        graph_builder.set_entry_point("generator")

        # Compile and run the app
        app = graph_builder.compile()

        # Example Invocation:
        final_output = app.invoke(
            {"oracle_code": oracle_sp, "max_attempts": 3, "attempts": 0}
        )
        return final_output

    def to_jsonable(self, obj: Any) -> Any:
        """Best-effort conversion for non-JSON types (Decimal, UUID, datetime, etc.)."""
        # psycopg will usually deliver Python-native types; stringify unknowns
        try:
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)

    def execute_sql_stmts(self, stmts: list[str]):
        try:
            with psycopg.connect(self.uri, autocommit=True) as conn:
                with conn.cursor() as cur:

                    for s in stmts:
                        cur.execute(s)
                        logger.info(f"🪳 🟢 {cur.statusmessage}")

        except Exception as e:
            logger.error(f"🪳 🔴 {str(e)}")

    def run(self) -> list[dict]:

        roots = []
        if self.root is None:
            # process all the files
            roots = [
                os.path.splitext(f)[0]
                for f in os.listdir(os.path.join(self.base_dir, "in/"))
                if os.path.isfile(os.path.join(self.base_dir, "in", f))
                and f.lower().endswith(".ddl")
            ]
        else:
            roots.append(self.root)

        for root in roots:
            self.convert(root)

    def convert(self, root: str) -> list[dict]:

        logger.info(f"🚀 Processing {root=}")

        self.root = root

        # create or override the out file
        with open(f"{self.base_dir}/out/{root}.out", "w") as f:
            pass

        if not os.path.exists(f"{os.path.join(self.base_dir,'in/', root)}.json"):
            logger.error(f"💾 ❌ Couldn't find expected output file {root}.json")
            return

        if not os.path.exists(f"{os.path.join(self.base_dir, 'in/', root)}.sql"):
            logger.error(f"💾 ❌ Couldn't find test statements file {root}.sql")
            return

        with open(f"{self.base_dir}/in/{root}.ddl", "r") as f:
            oracle_sp = f.read()

        with open(f"{self.base_dir}/in/{root}.json", "r") as f:
            self.expected_output = json.loads(f.read())

        with open(f"{self.base_dir}/in/{root}.sql", "r") as f:

            # separate seed files from seed statements from test statements

            txt = f.read()

            # separate seed file, if any
            if "betwixt_file_end" in txt:
                seed_files = [
                    x
                    for x in txt.split("betwixt_file_end")[0].split("\n")
                    if x.strip().endswith(".sql")
                ]

                for s in seed_files:
                    if not os.path.isfile(os.path.join(self.base_dir, "in", s)):
                        logger.warning(f"💾 ⚠️ File <{s}> not found")
                        return

                    with open(os.path.join(self.base_dir, "in", s), "r") as f:
                        self.seed_statements += sqlparse.split(f.read())

                # remove file lines
                txt = txt.split("betwixt_file_end")[1]

            # separate individual seeding SQL statements
            if "betwixt_seed_end" in txt:
                self.seed_statements += sqlparse.split(txt.split("betwixt_seed_end")[0])

                # remove sql stmts lines
                txt = txt.split("betwixt_seed_end")[1]

            # the remaining SQL statements are TEST statements
            self.test_statements = sqlparse.split(txt)

        if len(self.expected_output.keys()) != len(self.test_statements):
            logger.error(
                f"❌ Expected output and Test statement count should match. "
                f"Expected count={len(self.expected_output)}, "
                f"Statement count={len(self.test_statements)}"
            )

            with open(f"{self.base_dir}/out/{root}.out", "a") as f:
                f.write(
                    f"Expected output and Test statement count should match. "
                    f"Expected count={len(self.expected_output)}, ",
                    f"Statement count={len(self.test_statements)}",
                )
                f.write("\n\n")

            return

        answer = self.langgraph_it(oracle_sp)

        logger.info(f"💾 Saved output to file out/{root}.out")

        with open(f"{self.base_dir}/out/{root}.ai.yaml", "w") as f:
            f.write(yaml.safe_dump(answer))
        logger.info(f"💾 Saved AI answer to file out/{root}.ai.yaml")

        with open(f"{self.base_dir}/out/{root}.ddl", "w") as f:
            f.write(answer["converted_code"])
        logger.info(f"💾 Saved converted code to file out/{root}.ddl")


# TODO llmlingua the prompt to save tokens
# TODO improve prompts syntax
