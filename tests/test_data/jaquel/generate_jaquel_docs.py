#!/usr/bin/env python3
"""
Generate markdown documentation from JAQuel test files
"""

import json
import re
from pathlib import Path


def parse_filename(filename):
    """Parse filename to extract numbers and text parts"""
    # Remove file extensions
    name = filename.replace(".json.proto", "").replace(".json", "")

    # Split by numbers to get parts
    parts = re.split(r"(\d+)", name)

    # Filter out empty parts and create structure
    filtered_parts = [p.strip() for p in parts if p.strip()]

    return filtered_parts


def organize_files():
    """Organize files by their hierarchical structure"""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    jaquel_dir = script_dir  # The script is already in the jaquel directory
    files = {}

    for file in jaquel_dir.glob("*.json"):
        if file.name.endswith(".json.proto"):
            continue

        parts = parse_filename(file.name)
        if len(parts) >= 6:  # Ensure we have enough parts
            # Extract main numbers and headings
            main_num = parts[0]
            main_heading = parts[1]
            sub_num = parts[2]
            sub_heading = parts[3]
            detail_num = parts[4]
            detail_text = " ".join(parts[5:])  # Rest as detail

            key = (main_num, main_heading, sub_num, sub_heading, detail_num, detail_text)

            # Read both JSON and proto files
            json_file = file
            proto_file = jaquel_dir / f"{file.name}.proto"

            json_content = ""
            proto_content = ""

            if json_file.exists():
                with open(json_file, "r") as f:
                    json_content = f.read()

            if proto_file.exists():
                with open(proto_file, "r") as f:
                    proto_content = f.read()

            files[key] = {"json": json_content, "proto": proto_content}

    return files


def generate_notebook():
    """Generate a Jupyter notebook executing the JAQuel queries"""
    files = organize_files()

    # Sort files by their keys
    sorted_files = sorted(files.items())

    notebook = {
        "cells": [],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.8.5"},
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }

    # Add title cell
    notebook["cells"].append(
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# JAQuel Query Examples - Interactive Execution\n"
                "\n"
                "This notebook demonstrates how to execute JAQuel queries using the odsbox library.\n"
                "Each query from the test files is executed using `con_i.query_data()`.\n"
                "\n"
                "**Note:** You need to configure the connection parameters (URL, auth) for your ODS server."
            ],
        }
    )

    # Add setup cell
    notebook["cells"].append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": ["from odsbox import ConI\n" "\n" 'con_i = ConI(url="SERVERURL", auth=("USER", "PASSWORD"))'],
        }
    )

    current_main = None
    current_sub = None

    for key, content in sorted_files:
        main_num, main_heading, sub_num, sub_heading, detail_num, detail_text = key

        # Main heading
        if current_main != (main_num, main_heading):
            notebook["cells"].append({"cell_type": "markdown", "metadata": {}, "source": f"## {main_heading}"})
            current_main = (main_num, main_heading)
            current_sub = None

        # Sub heading
        if current_sub != (sub_num, sub_heading):
            notebook["cells"].append({"cell_type": "markdown", "metadata": {}, "source": f"### {sub_heading}"})
            current_sub = (sub_num, sub_heading)

        # Detail section with query execution
        notebook["cells"].append({"cell_type": "markdown", "metadata": {}, "source": f"#### {detail_text}"})

        # Parse and execute the JSON query
        query = json.loads(content["json"])
        query_str = json.dumps(query, indent=2)

        notebook["cells"].append(
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": f"# {detail_text}\ncon_i.query_data({query_str})",
            }
        )

    # Add logout cell
    notebook["cells"].append(
        {"cell_type": "markdown", "metadata": {}, "source": "## Cleanup\n\nDon't forget to logout when finished:"}
    )

    notebook["cells"].append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": '# Logout and cleanup\ncon_i.logout()\nprint("Logged out successfully.")',
        }
    )

    return notebook


def generate_markdown():
    """Generate the markdown documentation"""
    files = organize_files()

    # Sort files by their keys
    sorted_files = sorted(files.items())

    markdown = "# JAQuel Query Examples\n\n"
    markdown += "This document contains examples of JAQuel queries and their corresponding "
    markdown += "ASAM ODS SelectStatement protobuf message serialized as JSON.\n\n"

    current_main = None
    current_sub = None

    for key, content in sorted_files:
        main_num, main_heading, sub_num, sub_heading, detail_num, detail_text = key

        # Main heading
        if current_main != (main_num, main_heading):
            markdown += f"\n## {main_heading}\n\n"
            current_main = (main_num, main_heading)
            current_sub = None

        # Sub heading
        if current_sub != (sub_num, sub_heading):
            markdown += f"\n### {sub_heading}\n\n"
            current_sub = (sub_num, sub_heading)

        # Detail section
        markdown += f"\n#### {detail_text}\n\n"

        # Code block with tabs
        markdown += "````{tab-set}\n\n"

        # JAQuel Query tab
        markdown += "```{tab-item} JAQuel Query\n\n"
        markdown += "```json\n"
        markdown += content["json"]
        markdown += "\n```\n\n"

        # Protocol Buffer Response tab
        markdown += "```{tab-item} ODS SelectStatement\n\n"
        markdown += "```json\n"
        markdown += content["proto"]
        markdown += "\n```\n\n"

        markdown += "````\n\n"

    return markdown


if __name__ == "__main__":
    # Generate markdown documentation
    markdown_content = generate_markdown()

    # Get the script directory and navigate to docs directory
    script_dir = Path(__file__).parent
    # Navigate up from tests/test_data/jaquel to project root, then to docs
    docs_dir = script_dir.parent.parent.parent / "docs"

    # Write markdown file
    markdown_file = docs_dir.resolve() / "jaquel_examples.md"
    with open(markdown_file, "w") as f:
        f.write(markdown_content)

    print(f"Generated {markdown_file}")
    print(f"Total length: {len(markdown_content)} characters")

    # Generate Jupyter notebook
    notebook_content = generate_notebook()

    # Write notebook file
    notebook_file = docs_dir.resolve() / "jaquel_examples_notebook.ipynb"
    with open(notebook_file, "w") as f:
        json.dump(notebook_content, f, indent=2)

    print(f"Generated {notebook_file}")
    print(f"Total cells: {len(notebook_content['cells'])}")
