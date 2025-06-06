"""
Cortex Analyst App
====================
This app allows users to interact with their data using natural language.
"""

import json  # To handle JSON data
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import _snowflake  # For interacting with Snowflake-specific APIs
import pandas as pd
import streamlit as st  # Streamlit library for building the web app
from snowflake.snowpark.context import (
    get_active_session,
)  # To interact with Snowflake sessions
from snowflake.snowpark.exceptions import SnowparkSQLException

# List of available semantic model paths in the format: <DATABASE>.<SCHEMA>.<STAGE>/<FILE-NAME>
# Each path points to a YAML file defining a semantic model
AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "STEPHANE_SANDBOX.PUBLIC.RAW_DATA/semantic_model_automated.yaml",
    "STEPHANE_SANDBOX.PUBLIC.RAW_DATA/semantic_model.yml",
]
API_ENDPOINT = "/api/v2/cortex/analyst/message"
FEEDBACK_API_ENDPOINT = "/api/v2/cortex/analyst/feedback"
API_COMPLETE_ENDPOINT = "/api/v2/cortex/inference:complete"
API_TIMEOUT = 50000  # in milliseconds

# Initialize a Snowpark session for executing queries
session = get_active_session()
setup_message = """
    You are an expert Neo4j Graph Analytics for Snowflake SQL generator. Your task is to interpret user requests for graph analysis and generate a single, complete SQL query.

    **Key Guidelines and Constraints:**

    1.  **Output Format:** Your response MUST be a valid SQL query for Neo4j Graph Analytics for Snowflake. Do not include any conversational text, explanations, or code blocks other than the SQL query itself.
    2.  **Algorithm Selection:**
        * Choose the most appropriate graph algorithm from the available list: `BETWEENNESS_CENTRALITY`, `DEGREE_CENTRALITY`, `DIJKSTRA_SINGLE_SOURCE_SHORTEST_PATH`, `DIJKSTRA_SOURCE_TARGET_SHORTEST_PATH`, `FASTRP`, `FASTPATH`, `GRAPHSAGE`, `K_NEAREST_NEIGHBORS`, `LOUVAIN`, `NODE_CLASSIFICATION_TRAINING`, `NODE_CLASSIFICATION_PREDICTION`, `NODE_EMBEDDINGS_TRAINING`, `NODE_EMBEDDINGS_PREDICTION`, `NODE_SIMILARITY`, `PAGERANK`, `TRIANGLE_COUNT`, `WEAKLY_CONNECTED_COMPONENTS`.
        * Prioritize algorithms that directly address community detection, centrality, pathfinding, or embeddings based on the user's intent.
    3.  **SQL Structure:** The SQL query will adhere to the following pattern for calling Neo4j Graph Analytics:

        ```sql
        CALL NEO4J_GRAPH_ANALYTICS.ADMIN.RUN_JOB(
            'YOUR_APP_NAME',
            '<ALGORITHM_NAME>',
            OBJECT_CONSTRUCT(
                'node_table', 'YOUR_NODES_TABLE',
                'relationship_table', 'YOUR_RELATIONSHIPS_TABLE',
                'output_table', 'YOUR_OUTPUT_TABLE'
                -- Add algorithm-specific parameters here, e.g., 'source_node_id', 'target_node_id', 'weight_property', 'iterations', 'embedding_size', etc.
            )
        );
        ```
        * `YOUR_APP_NAME` should always be `NEO4J_GRAPH_ANALYTICS`.
        * `<ALGORITHM_NAME>` must be one of the exact algorithm names listed in guideline 2, enclosed in single quotes.
        * `YOUR_NODES_TABLE` and `YOUR_RELATIONSHIPS_TABLE` will be identified from the provided semantic context. These should be fully qualified (e.g., `DATABASE.SCHEMA.TABLE_NAME`).
        * `YOUR_OUTPUT_TABLE` should be a new, descriptive table name in the format `APP_SCHEMA.ALGORITHM_OUTPUT_<TIMESTAMP_OR_DESCRIPTIVE_SUFFIX>`. For example, `GRAPH_APP_RESULTS.LOUVAIN_COMMUNITIES_202506041621`. Ensure the output table name is distinct and descriptive.
        * You MUST infer and include any necessary algorithm-specific parameters within the `OBJECT_CONSTRUCT` based on the user's request and the semantic context. Common parameters include:
            * `source_node_id` (for shortest path, typically a column from the nodes table)
            * `target_node_id` (for shortest path, typically a column from the nodes table)
            * `weight_property` (for weighted algorithms, typically a numeric column from the relationships table)
            * `iterations` (for iterative algorithms like PageRank)
            * `embedding_size` (for embedding algorithms)
            * `label_property` (for node classification)
            * `relationship_type_property` (if relationships have types)
            * `node_id_property` (if node IDs are not the primary key)
            * `source_node_property` (for relationship table mapping, e.g., 'FROM_NODE_ID')
            * `target_node_property` (for relationship table mapping, e.g., 'TO_NODE_ID')
    4.  **Semantic Context Usage:** The user will provide a "semantic file" containing metadata about available tables and their relationships. You MUST parse this context to:
        * Identify appropriate node and relationship tables for the requested analysis.
        * Infer column names for node IDs, relationship source/target, and any properties (like weights) needed by the chosen algorithm.
        * **Crucially, only use tables and columns explicitly defined in the semantic context.** If a suitable table or column is not found for a requested analysis, indicate that the request cannot be fulfilled with the current data schema.
    5.  **Error Handling (Implicit):** If a user's request cannot be translated into a valid Neo4j Graph Analytics SQL query given the available algorithms and semantic context, output a clear message indicating why the query cannot be generated and what information is missing. For example: "Error: Cannot generate SQL. The request for [ALGORITHM] requires a [PROPERTY/TABLE] that is not defined in the provided semantic context."

    **Semantic File Format Expectation (for LLM to parse):**

    The semantic file will be provided in a JSON-like format within the user's prompt. It will describe databases, schemas, tables, columns, and their inferred roles (e.g., `node_table`, `relationship_table`, `node_id`, `relationship_source_id`, `relationship_target_id`, `properties`).
"""


def main():
    # Initialize session state
    if "messages" not in st.session_state:
        reset_session_state()
    show_header_and_sidebar()
    if len(st.session_state.messages) == 0:
        st.session_state.messages.append(
            {
                "role": "system",
                "content": [{"type": "text", "text": setup_message}],
            }
        )
        process_user_input("What questions can I ask?")
    display_conversation()
    handle_user_inputs()
    handle_error_notifications()
    display_warnings()


def reset_session_state():
    """Reset important session state elements."""
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.active_suggestion = None  # Currently selected suggestion
    st.session_state.warnings = []  # List to store warnings
    st.session_state.form_submitted = (
        {}
    )  # Dictionary to store feedback submission for each request


def show_header_and_sidebar():
    """Display the header and sidebar of the app."""
    # Set the title and introductory text of the app
    st.title("Cortex Analyst")
    st.markdown(
        "Welcome to Cortex Analyst! Type your questions below to interact with your data. "
    )

    # Sidebar with a reset button
    with st.sidebar:
        st.selectbox(
            "Selected semantic model:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )
        st.divider()
        # Center this button
        _, btn_container, _ = st.columns([2, 6, 2])
        if btn_container.button("Clear Chat History", use_container_width=True):
            reset_session_state()


def handle_user_inputs():
    """Handle user inputs from the chat interface."""
    # Handle chat input
    user_input = st.chat_input("What is your question?")
    if user_input:
        process_user_input(user_input)
    # Handle suggested question click
    elif st.session_state.active_suggestion is not None:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion)


def handle_error_notifications():
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occured!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False


def process_user_input(prompt: str):
    """
    Process user input and update the conversation history by running two SQL queries sequentially.

    Args: I HATE LIFE ME TOO
        prompt (str): The user's input.
    """
    # Clear previous warnings at the start of a new request
    st.session_state.warnings = []

    # Create a new message, append to history and display immediately
    new_user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
    }
    st.session_state.messages.append(new_user_message)
    with st.chat_message("user"):
        user_msg_index = len(st.session_state.messages) - 1
        display_message(new_user_message["content"], user_msg_index)

    # First SQL Query: create_relevant_graph_tables
    with st.chat_message("analyst"):
        with st.spinner("Waiting for Graph Analyst's response (Query 1/2)..."):
            time.sleep(1)
            response1, error_msg1 = create_relevant_graph_tables(
                st.session_state.messages
            )

            analyst_message1 = {
                "role": "analyst",
                "content": (
                    response1["message"]["content"]
                    if error_msg1 is None
                    else [{"type": "text", "text": error_msg1}]
                ),
                "request_id": response1["request_id"],
            }
            if error_msg1 is not None:
                st.session_state["fire_API_error_notify"] = True

            if "warnings" in response1:
                st.session_state.warnings.extend(
                    response1["warnings"]
                )  # Use extend to add all warnings

            st.session_state.messages.append(analyst_message1)
            # Rerun is handled after both queries for a single display update
            st.rerun()

    #         bridge_user_message = {
    #             "role": "user",
    #             "content": [{"type": "text", "text": "Please take my previous prompt and perform an analysis"}],
    #         }
    #         st.session_state.messages.append(bridge_user_message)

    # # Second SQL Query: get_analyst_response
    #     with st.spinner("Waiting for Analyst's response (Query 2/2)..."):
    #         time.sleep(1)
    #         response2, error_msg2 = get_analyst_response(st.session_state.messages)

    #         analyst_message2 = {
    #             "role": "analyst",
    #             "content": response2["message"]["content"] if error_msg2 is None else [{"type": "text", "text": error_msg2}],
    #             "request_id": response2["request_id"],
    #         }
    #         if error_msg2 is not None:
    #             st.session_state["fire_API_error_notify"] = True

    #         if "warnings" in response2:
    #             st.session_state.warnings.extend(response2["warnings"]) # Use extend to add all warnings

    #         st.session_state.messages.append(analyst_message2)
    #         st.rerun()


def display_warnings():
    """
    Display warnings to the user.
    """
    warnings = st.session_state.warnings
    for warning in warnings:
        st.warning(warning["message"], icon="⚠️")


def create_relevant_graph_tables(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    """
    Send chat history to LLM and return the response.

    Args:
        messages (List[Dict]): The conversation history.

    Returns:
        Optional[Dict]: The response from the Cortex Analyst API.
    """
    # Prepare the request body with the user's prompt
    request_body = {
        "messages": messages,
        "model": "mistral-large2",
    }

    # Send a POST request to the Cortex Analyst API endpoint
    # Adjusted to use positional arguments as per the API's requirement
    resp = _snowflake.send_snow_api_request(
        "POST",  # method
        API_COMPLETE_ENDPOINT,  # path
        {},  # headers
        {},  # params
        request_body,  # body
        None,  # request_guid
        API_TIMEOUT,  # timeout in milliseconds
    )

    # Content is a string with serialized JSON object
    parsed_content = json.loads(resp["content"])

    # Check if the response is successful
    # Check if the response is successful
    if resp["status"] < 400:
        # Return the content of the response as a JSON object
        return parsed_content, None
    else:
        # Craft readable error message
        error_msg = f"""
🚨 A model creation error has occurred 🚨

* response code: `{resp['status']}`
* request-id: `{parsed_content['request_id']}`
* error code: `{parsed_content['error_code']}`

Message:
```
{parsed_content['message']}
```
        """
        return parsed_content, error_msg


def get_analyst_response(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    """
    Send chat history to the Cortex Analyst API and return the response.

    Args:
        messages (List[Dict]): The conversation history.

    Returns:
        Optional[Dict]: The response from the Cortex Analyst API.
    """
    # Prepare the request body with the user's prompt
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }

    # Send a POST request to the Cortex Analyst API endpoint
    # Adjusted to use positional arguments as per the API's requirement
    resp = _snowflake.send_snow_api_request(
        "POST",  # method
        API_ENDPOINT,  # path
        {},  # headers
        {},  # params
        request_body,  # body
        None,  # request_guid
        API_TIMEOUT,  # timeout in milliseconds
    )

    # Content is a string with serialized JSON object
    parsed_content = json.loads(resp["content"])

    # Check if the response is successful
    if resp["status"] < 400:
        # Return the content of the response as a JSON object
        return parsed_content, None
    else:
        # Craft readable error message
        error_msg = f"""
🚨 An Analyst API error has occurred 🚨

* response code: `{resp['status']}`
* request-id: `{parsed_content['request_id']}`
* error code: `{parsed_content['error_code']}`

Message:
```
{parsed_content['message']}
```
        """
        return parsed_content, error_msg


def display_conversation():
    """
    Display the conversation history between the user and the assistant.
    """
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            if role == "analyst":
                display_message(content, idx, message["request_id"])
            else:
                display_message(content, idx)


def display_message(
    content: List[Dict[str, Union[str, Dict]]],
    message_index: int,
    request_id: Union[str, None] = None,
):
    """
    Display a single message content.

    Args:
        content (List[Dict[str, str]]): The message content.
        message_index (int): The index of the message.
    """
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            # Display suggestions as buttons
            for suggestion_index, suggestion in enumerate(item["suggestions"]):
                if st.button(
                    suggestion, key=f"suggestion_{message_index}_{suggestion_index}"
                ):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            # Display the SQL query and results
            display_sql_query(
                item["statement"], message_index, item["confidence"], request_id
            )
        else:
            # Handle other content types if necessary
            pass


@st.cache_data(show_spinner=False)
def get_query_exec_result(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Execute the SQL query and convert the results to a pandas DataFrame.

    Args:
        query (str): The SQL query.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: The query results and the error message.
    """
    global session
    try:
        df = session.sql(query).to_pandas()
        return df, None
    except SnowparkSQLException as e:
        return None, str(e)


def display_sql_confidence(confidence: dict):
    if confidence is None:
        return
    verified_query_used = confidence["verified_query_used"]
    with st.popover(
        "Verified Query Used",
        help="The verified query from [Verified Query Repository](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/verified-query-repository), used to generate the SQL",
    ):
        with st.container():
            if verified_query_used is None:
                st.text(
                    "There is no query from the Verified Query Repository used to generate this SQL answer"
                )
                return
            st.text(f"Name: {verified_query_used['name']}")
            st.text(f"Question: {verified_query_used['question']}")
            st.text(f"Verified by: {verified_query_used['verified_by']}")
            st.text(
                f"Verified at: {datetime.fromtimestamp(verified_query_used['verified_at'])}"
            )
            st.text("SQL query:")
            st.code(verified_query_used["sql"], language="sql", wrap_lines=True)


def display_sql_query(
    sql: str, message_index: int, confidence: dict, request_id: Union[str, None] = None
):
    """
    Executes the SQL query and displays the results in form of data frame and charts.

    Args:
        sql (str): The SQL query.
        message_index (int): The index of the message.
        confidence (dict): The confidence information of SQL query generation
        request_id (str): Request id from user request
    """

    # Display the SQL query
    with st.expander("SQL Query", expanded=False):
        st.code(sql, language="sql")
        display_sql_confidence(confidence)

    # Display the results of the SQL query
    with st.expander("Results", expanded=True):
        with st.spinner("Running SQL..."):
            df, err_msg = get_query_exec_result(sql)
            if df is None:
                st.error(f"Could not execute generated SQL query. Error: {err_msg}")
            elif df.empty:
                st.write("Query returned no data")
            else:
                # Show query results in two tabs
                data_tab, chart_tab = st.tabs(["Data 📄", "Chart 📉"])
                with data_tab:
                    st.dataframe(df, use_container_width=True)

                with chart_tab:
                    display_charts_tab(df, message_index)
    if request_id:
        display_feedback_section(request_id)


def display_charts_tab(df: pd.DataFrame, message_index: int) -> None:
    """
    Display the charts tab.

    Args:
        df (pd.DataFrame): The query results.
        message_index (int): The index of the message.
    """
    # There should be at least 2 columns to draw charts
    if len(df.columns) >= 2:
        all_cols_set = set(df.columns)
        col1, col2 = st.columns(2)
        x_col = col1.selectbox(
            "X axis", all_cols_set, key=f"x_col_select_{message_index}"
        )
        y_col = col2.selectbox(
            "Y axis",
            all_cols_set.difference({x_col}),
            key=f"y_col_select_{message_index}",
        )
        chart_type = st.selectbox(
            "Select chart type",
            options=["Line Chart 📈", "Bar Chart 📊"],
            key=f"chart_type_{message_index}",
        )
        if chart_type == "Line Chart 📈":
            st.line_chart(df.set_index(x_col)[y_col])
        elif chart_type == "Bar Chart 📊":
            st.bar_chart(df.set_index(x_col)[y_col])
    else:
        st.write("At least 2 columns are required")


def display_feedback_section(request_id: str):
    with st.popover("📝 Query Feedback"):
        if request_id not in st.session_state.form_submitted:
            with st.form(f"feedback_form_{request_id}", clear_on_submit=True):
                positive = st.radio(
                    "Rate the generated SQL", options=["👍", "👎"], horizontal=True
                )
                positive = positive == "👍"
                submit_disabled = (
                    request_id in st.session_state.form_submitted
                    and st.session_state.form_submitted[request_id]
                )

                feedback_message = st.text_input("Optional feedback message")
                submitted = st.form_submit_button("Submit", disabled=submit_disabled)
                if submitted:
                    err_msg = submit_feedback(request_id, positive, feedback_message)
                    st.session_state.form_submitted[request_id] = {"error": err_msg}
                    st.session_state.popover_open = False
                    st.rerun()
        elif (
            request_id in st.session_state.form_submitted
            and st.session_state.form_submitted[request_id]["error"] is None
        ):
            st.success("Feedback submitted", icon="✅")
        else:
            st.error(st.session_state.form_submitted[request_id]["error"])


def submit_feedback(
    request_id: str, positive: bool, feedback_message: str
) -> Optional[str]:
    request_body = {
        "request_id": request_id,
        "positive": positive,
        "feedback_message": feedback_message,
    }
    resp = _snowflake.send_snow_api_request(
        "POST",  # method
        FEEDBACK_API_ENDPOINT,  # path
        {},  # headers
        {},  # params
        request_body,  # body
        None,  # request_guid
        API_TIMEOUT,  # timeout in milliseconds
    )
    if resp["status"] == 200:
        return None

    parsed_content = json.loads(resp["content"])
    # Craft readable error message
    err_msg = f"""
        🚨 An Analyst API error has occurred 🚨
        
        * response code: `{resp['status']}`
        * request-id: `{parsed_content['request_id']}`
        * error code: `{parsed_content['error_code']}`
        
        Message:
        ```
        {parsed_content['message']}
        ```
        """
    return err_msg


if __name__ == "__main__":
    main()
